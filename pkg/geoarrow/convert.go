package geoarrow

import (
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// convertBatch replaces WKB binary column(s) with native GeoArrow column(s).
// Returns a new RecordBatch (caller must Release). The geoType is auto-detected
// on first non-null geometry; subsequent calls should pass the same geoType.
func ConvertBatch(
	rec arrow.RecordBatch,
	col GeometryColumn,
	geoType GeoType,
	mem memory.Allocator,
) (arrow.RecordBatch, GeoType, error) {
	binArr, ok := rec.Column(col.Index).(*array.Binary)
	if !ok {
		// Try LargeBinary
		return rec, geoType, nil
	}

	numRows := int(rec.NumRows())

	// ── Pass 1: count ──
	var reader wkbReader
	totalCoords := 0
	totalRings := 0
	totalParts := 0
	nullCount := 0

	for i := 0; i < numRows; i++ {
		if binArr.IsNull(i) {
			nullCount++
			continue
		}
		wkb := binArr.Value(i)
		reader.reset(wkb)
		counts, wkbType, ok := reader.countGeometry()
		if !ok {
			nullCount++
			continue
		}

		// Detect type from first valid geometry
		if geoType == GeoTypeUnknown {
			geoType = classifyWkbType(wkbType)
			if geoType == GeoTypeUnknown {
				// Unsupported type — skip conversion
				return rec, GeoTypeUnknown, nil
			}
		}

		// Check compatibility
		rowType := classifyWkbType(wkbType)
		if rowType != geoType {
			// Mixed hierarchy — skip conversion
			return rec, GeoTypeUnknown, nil
		}

		totalCoords += counts.coords
		totalRings += counts.rings

		// For Multi* promotion: single geom = 1 part
		switch wkbType {
		case wkbPoint, wkbLineString, wkbPolygon:
			totalParts++
		default:
			totalParts += counts.parts
		}
	}

	if geoType == GeoTypeUnknown {
		return rec, geoType, nil
	}

	// ── Pass 2: fill pre-allocated buffers ──
	coords := make([]float64, totalCoords*2)
	nullBitmap := make([]byte, (numRows+7)/8)

	// Offset arrays depend on geometry type
	var state fillState
	state.coords = coords

	// geomOff: per-row → part index (always needed for Multi*)
	geomOff := make([]int32, numRows+1)

	switch geoType {
	case GeoTypePoint:
		partOff := make([]int32, totalParts+1)
		state.partOff = partOff

	case GeoTypeLine:
		partOff := make([]int32, totalParts+1)
		state.partOff = partOff

	case GeoTypePolygon:
		ringOff := make([]int32, totalRings+1)
		partOff := make([]int32, totalParts+1)
		state.ringOff = ringOff
		state.partOff = partOff
	}

	state.geomOff = geomOff

	for i := 0; i < numRows; i++ {
		if binArr.IsNull(i) {
			geomOff[i+1] = geomOff[i]
			continue
		}
		wkb := binArr.Value(i)
		reader.reset(wkb)

		partsBefore := state.partPos
		ok := reader.fillGeometry(&state, geoType)
		if !ok {
			// Failed to parse — treat as null
			geomOff[i+1] = geomOff[i]
			continue
		}

		// Set null bitmap
		nullBitmap[i/8] |= 1 << (uint(i) % 8)

		// geomOff meaning depends on type:
		// Point: coord index (into FixedSizeList)
		// Line: part index (into inner List)
		// Polygon: part index (into inner List of Lists)
		switch geoType {
		case GeoTypePoint:
			geomOff[i+1] = int32(state.coordPos / 2)
		default:
			geomOff[i+1] = int32(state.partPos)
		}
		_ = partsBefore
	}

	// ── Build Arrow arrays bottom-up ──
	geoArr := buildGeoArrowArray(geoType, coords, state.partOff, state.ringOff, geomOff, nullBitmap, numRows, nullCount)
	if geoArr == nil {
		return rec, geoType, nil
	}
	defer geoArr.Release()

	// Build new schema with GeoArrow field
	newField, err := GeoArrowField(col.Name, geoType, col.ExtensionMeta)
	if err != nil {
		return rec, geoType, err
	}

	fields := make([]arrow.Field, len(rec.Schema().Fields()))
	copy(fields, rec.Schema().Fields())
	fields[col.Index] = newField
	meta := rec.Schema().Metadata()
	newSchema := arrow.NewSchema(fields, &meta)

	// Build new column list
	cols := make([]arrow.Array, rec.NumCols())
	for i := 0; i < int(rec.NumCols()); i++ {
		if i == col.Index {
			cols[i] = geoArr
		} else {
			cols[i] = rec.Column(i)
		}
	}

	newRec := array.NewRecordBatch(newSchema, cols, int64(numRows))
	return newRec, geoType, nil
}

// buildGeoArrowArray constructs the nested Arrow array from pre-allocated buffers.
func buildGeoArrowArray(
	geoType GeoType,
	coords []float64,
	partOff []int32,
	ringOff []int32,
	geomOff []int32,
	nullBitmap []byte,
	numRows int,
	nullCount int,
) arrow.Array {
	// Bottom: flat Float64 coordinate values
	coordBuf := memory.NewBufferBytes(float64SliceToBytes(coords))
	coordData := array.NewData(
		arrow.PrimitiveTypes.Float64,
		len(coords),
		[]*memory.Buffer{nil, coordBuf},
		nil, 0, 0,
	)
	defer coordData.Release()

	// FixedSizeList[2] wrapping coordinates
	fslData := array.NewData(
		CoordType,
		len(coords)/2,
		[]*memory.Buffer{nil},
		[]arrow.ArrayData{coordData},
		0, 0,
	)
	defer fslData.Release()

	switch geoType {
	case GeoTypePoint:
		// MultiPoint: List<FSL[2]>
		// geomOff[i] = coord index (into fsl) for row i
		// For single points geomOff goes [0, 1, 2, ...], for multi [0, 3, 5, ...]
		geomBuf := memory.NewBufferBytes(int32SliceToBytes(geomOff))
		nullBuf := memory.NewBufferBytes(nullBitmap)
		outerData := array.NewData(
			MultiPointType,
			numRows,
			[]*memory.Buffer{nullBuf, geomBuf},
			[]arrow.ArrayData{fslData},
			nullCount, 0,
		)
		defer outerData.Release()
		return array.NewListData(outerData)

	case GeoTypeLine:
		// MultiLineString: List<List<FSL[2]>>
		// partOff = coord offsets per linestring
		partBuf := memory.NewBufferBytes(int32SliceToBytes(partOff))
		lineListData := array.NewData(
			arrow.ListOf(CoordType),
			len(partOff)-1,
			[]*memory.Buffer{nil, partBuf},
			[]arrow.ArrayData{fslData},
			0, 0,
		)
		defer lineListData.Release()
		// geomOff = part offsets per row
		geomBuf := memory.NewBufferBytes(int32SliceToBytes(geomOff))
		nullBuf := memory.NewBufferBytes(nullBitmap)
		outerData := array.NewData(
			MultiLineStringType,
			numRows,
			[]*memory.Buffer{nullBuf, geomBuf},
			[]arrow.ArrayData{lineListData},
			nullCount, 0,
		)
		defer outerData.Release()
		return array.NewListData(outerData)

	case GeoTypePolygon:
		// MultiPolygon: List<List<List<FSL[2]>>>
		// ringOff = coord offsets per ring
		ringBuf := memory.NewBufferBytes(int32SliceToBytes(ringOff))
		ringListData := array.NewData(
			arrow.ListOf(CoordType),
			len(ringOff)-1,
			[]*memory.Buffer{nil, ringBuf},
			[]arrow.ArrayData{fslData},
			0, 0,
		)
		defer ringListData.Release()
		// partOff = ring offsets per polygon
		partBuf := memory.NewBufferBytes(int32SliceToBytes(partOff))
		polyListData := array.NewData(
			arrow.ListOf(arrow.ListOf(CoordType)),
			len(partOff)-1,
			[]*memory.Buffer{nil, partBuf},
			[]arrow.ArrayData{ringListData},
			0, 0,
		)
		defer polyListData.Release()
		// geomOff = polygon offsets per row
		geomBuf := memory.NewBufferBytes(int32SliceToBytes(geomOff))
		nullBuf := memory.NewBufferBytes(nullBitmap)
		outerData := array.NewData(
			MultiPolygonType,
			numRows,
			[]*memory.Buffer{nullBuf, geomBuf},
			[]arrow.ArrayData{polyListData},
			nullCount, 0,
		)
		defer outerData.Release()
		return array.NewListData(outerData)
	}

	return nil
}

// float64SliceToBytes converts []float64 to []byte without copying.
func float64SliceToBytes(s []float64) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s)*8)
}

// int32SliceToBytes converts []int32 to []byte without copying.
func int32SliceToBytes(s []int32) []byte {
	if len(s) == 0 {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(&s[0])), len(s)*4)
}
