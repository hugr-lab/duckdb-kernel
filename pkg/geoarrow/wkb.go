// Package geoarrow provides zero-allocation WKB→GeoArrow conversion
// for Apache Arrow record batches.
//
// Usage:
//
//	converter := geoarrow.NewConverter(reader, geoarrow.WithBufferSize(2))
//	defer converter.Release()
//	// use converter as array.RecordReader
package geoarrow

import (
	"encoding/binary"
	"math"
)

// WKB geometry type constants.
const (
	wkbPoint              = 1
	wkbLineString         = 2
	wkbPolygon            = 3
	wkbMultiPoint         = 4
	wkbMultiLineString    = 5
	wkbMultiPolygon       = 6
	wkbGeometryCollection = 7
)

// GeoType represents the canonical geometry type for a column.
type GeoType int

const (
	GeoTypeUnknown GeoType = iota
	GeoTypePoint
	GeoTypeLine
	GeoTypePolygon
)

// wkbReader is a zero-allocation WKB parser that reads directly from a byte slice.
type wkbReader struct {
	buf   []byte
	pos   int
	order binary.ByteOrder
	dims  int // 2=XY, 3=XYZ, 4=XYZM
}

func (r *wkbReader) reset(buf []byte) {
	r.buf = buf
	r.pos = 0
	r.order = nil
	r.dims = 2
}

func (r *wkbReader) remaining() int {
	return len(r.buf) - r.pos
}

// readHeader parses WKB byte order + geometry type + optional SRID.
// Returns the base geometry type (1-7).
func (r *wkbReader) readHeader() (geomType int, ok bool) {
	if r.remaining() < 5 {
		return 0, false
	}

	// Byte order
	if r.buf[r.pos] == 1 {
		r.order = binary.LittleEndian
	} else {
		r.order = binary.BigEndian
	}
	r.pos++

	// Geometry type (4 bytes)
	gt := r.order.Uint32(r.buf[r.pos:])
	r.pos += 4

	// Handle EWKB SRID flag
	hasSRID := (gt & 0x20000000) != 0
	hasZ := (gt & 0x80000000) != 0
	hasM := (gt & 0x40000000) != 0

	// Strip flags
	baseType := int(gt & 0x1FFFFFFF)

	// Handle ISO WKB Z/M encoding
	if baseType >= 3000 {
		baseType -= 3000
		hasZ = true
		hasM = true
	} else if baseType >= 2000 {
		baseType -= 2000
		hasM = true
	} else if baseType >= 1000 {
		baseType -= 1000
		hasZ = true
	}

	r.dims = 2
	if hasZ {
		r.dims++
	}
	if hasM {
		r.dims++
	}

	// Skip SRID if present
	if hasSRID {
		if r.remaining() < 4 {
			return 0, false
		}
		r.pos += 4
	}

	return baseType, true
}

func (r *wkbReader) readUint32() (uint32, bool) {
	if r.remaining() < 4 {
		return 0, false
	}
	v := r.order.Uint32(r.buf[r.pos:])
	r.pos += 4
	return v, true
}

func (r *wkbReader) readFloat64() (float64, bool) {
	if r.remaining() < 8 {
		return 0, false
	}
	v := math.Float64frombits(r.order.Uint64(r.buf[r.pos:]))
	r.pos += 8
	return v, true
}

// skipCoords advances past n coordinates without reading them.
// Returns false if buffer is too short.
func (r *wkbReader) skipCoords(n int) bool {
	need := n * r.dims * 8
	if r.remaining() < need {
		return false
	}
	r.pos += need
	return true
}

// readCoordsXY reads n coordinates, copying only X,Y into dst.
// Returns the number of float64 values written (n*2), or -1 on error.
func (r *wkbReader) readCoordsXY(dst []float64, n int) int {
	// Pre-check: need n * dims * 8 bytes
	if r.remaining() < n*r.dims*8 || len(dst) < n*2 {
		return -1
	}
	w := 0
	for i := 0; i < n; i++ {
		dst[w] = math.Float64frombits(r.order.Uint64(r.buf[r.pos:]))
		r.pos += 8
		dst[w+1] = math.Float64frombits(r.order.Uint64(r.buf[r.pos:]))
		r.pos += 8
		w += 2
		// Skip Z and/or M
		for d := 2; d < r.dims; d++ {
			r.pos += 8
		}
	}
	return w
}

// countGeometry counts coordinates, rings, and parts for a single WKB geometry.
// It only reads headers and counts — no coordinate data is parsed.
type geomCounts struct {
	coords int
	rings  int
	parts  int
}

func (r *wkbReader) countGeometry() (geomCounts, int, bool) {
	gt, ok := r.readHeader()
	if !ok {
		return geomCounts{}, 0, false
	}

	var c geomCounts

	switch gt {
	case wkbPoint:
		c.coords = 1
		if !r.skipCoords(1) {
			return c, 0, false
		}
		return c, gt, true

	case wkbLineString:
		n, ok := r.readUint32()
		if !ok {
			return c, 0, false
		}
		c.coords = int(n)
		if !r.skipCoords(int(n)) {
			return c, 0, false
		}
		return c, gt, true

	case wkbPolygon:
		nRings, ok := r.readUint32()
		if !ok {
			return c, 0, false
		}
		c.rings = int(nRings)
		for i := 0; i < int(nRings); i++ {
			n, ok := r.readUint32()
			if !ok {
				return c, 0, false
			}
			c.coords += int(n)
			if !r.skipCoords(int(n)) {
				return c, 0, false
			}
		}
		return c, gt, true

	case wkbMultiPoint:
		nParts, ok := r.readUint32()
		if !ok {
			return c, 0, false
		}
		c.parts = int(nParts)
		for i := 0; i < int(nParts); i++ {
			sub, _, ok := r.countGeometry()
			if !ok {
				return c, 0, false
			}
			c.coords += sub.coords
		}
		return c, gt, true

	case wkbMultiLineString:
		nParts, ok := r.readUint32()
		if !ok {
			return c, 0, false
		}
		c.parts = int(nParts)
		for i := 0; i < int(nParts); i++ {
			sub, _, ok := r.countGeometry()
			if !ok {
				return c, 0, false
			}
			c.coords += sub.coords
		}
		return c, gt, true

	case wkbMultiPolygon:
		nParts, ok := r.readUint32()
		if !ok {
			return c, 0, false
		}
		c.parts = int(nParts)
		for i := 0; i < int(nParts); i++ {
			sub, _, ok := r.countGeometry()
			if !ok {
				return c, 0, false
			}
			c.coords += sub.coords
			c.rings += sub.rings
		}
		return c, gt, true

	case wkbGeometryCollection:
		nGeoms, ok := r.readUint32()
		if !ok {
			return c, 0, false
		}
		if nGeoms == 0 {
			return c, 0, true
		}
		resolvedType := 0
		for i := 0; i < int(nGeoms); i++ {
			sub, subType, ok := r.countGeometry()
			if !ok {
				return c, 0, false
			}
			baseSubType := subType
			if baseSubType >= wkbMultiPoint {
				baseSubType -= 3
			}
			if resolvedType == 0 {
				resolvedType = baseSubType
			} else if resolvedType != baseSubType {
				return c, 0, false
			}
			c.coords += sub.coords
			c.rings += sub.rings
			c.parts += sub.parts
			if sub.parts == 0 {
				c.parts++
			}
		}
		multiType := resolvedType + 3
		return c, multiType, true
	}

	return c, 0, false
}

// fillGeometry reads a WKB geometry and writes coordinates + offsets into pre-allocated buffers.
type fillState struct {
	coords     []float64
	coordPos   int
	ringOff    []int32
	ringPos    int
	partOff    []int32
	partPos    int
	geomOff    []int32
	geomIdx    int
}

func (r *wkbReader) fillPoint(s *fillState) bool {
	w := r.readCoordsXY(s.coords[s.coordPos:], 1)
	if w < 0 {
		return false
	}
	s.coordPos += 2
	return true
}

func (r *wkbReader) fillLineString(s *fillState) bool {
	n, ok := r.readUint32()
	if !ok {
		return false
	}
	w := r.readCoordsXY(s.coords[s.coordPos:], int(n))
	if w < 0 {
		return false
	}
	s.coordPos += int(n) * 2
	return true
}

func (r *wkbReader) fillPolygon(s *fillState) bool {
	nRings, ok := r.readUint32()
	if !ok {
		return false
	}
	for i := 0; i < int(nRings); i++ {
		n, ok := r.readUint32()
		if !ok {
			return false
		}
		w := r.readCoordsXY(s.coords[s.coordPos:], int(n))
		if w < 0 {
			return false
		}
		s.coordPos += int(n) * 2
		s.ringPos++
		s.ringOff[s.ringPos] = int32(s.coordPos / 2)
	}
	return true
}

// appendPartCoord appends a coord-based part offset.
func (s *fillState) appendPartCoord() {
	s.partPos++
	s.partOff[s.partPos] = int32(s.coordPos / 2)
}

// appendPartRing appends a ring-based part offset.
func (s *fillState) appendPartRing() {
	s.partPos++
	s.partOff[s.partPos] = int32(s.ringPos)
}

func (r *wkbReader) fillGeometry(s *fillState, geoType GeoType) bool {
	gt, ok := r.readHeader()
	if !ok {
		return false
	}

	switch gt {
	case wkbPoint:
		if !r.fillPoint(s) {
			return false
		}
		if geoType == GeoTypePoint {
			s.appendPartCoord()
		}

	case wkbLineString:
		if !r.fillLineString(s) {
			return false
		}
		if geoType == GeoTypeLine {
			s.appendPartCoord()
		}

	case wkbPolygon:
		if !r.fillPolygon(s) {
			return false
		}
		if geoType == GeoTypePolygon {
			s.appendPartRing()
		}

	case wkbMultiPoint:
		np, ok := r.readUint32()
		if !ok {
			return false
		}
		nParts := int(np)
		for i := 0; i < nParts; i++ {
			subGt, ok := r.readHeader()
			if !ok || subGt != wkbPoint {
				return false
			}
			if !r.fillPoint(s) {
				return false
			}
		}
		s.appendPartCoord()

	case wkbMultiLineString:
		np, ok := r.readUint32()
		if !ok {
			return false
		}
		nParts := int(np)
		for i := 0; i < nParts; i++ {
			subGt, ok := r.readHeader()
			if !ok || subGt != wkbLineString {
				return false
			}
			if !r.fillLineString(s) {
				return false
			}
			s.appendPartCoord()
		}

	case wkbMultiPolygon:
		np, ok := r.readUint32()
		if !ok {
			return false
		}
		nParts := int(np)
		for i := 0; i < nParts; i++ {
			subGt, ok := r.readHeader()
			if !ok || subGt != wkbPolygon {
				return false
			}
			if !r.fillPolygon(s) {
				return false
			}
			s.appendPartRing()
		}

	case wkbGeometryCollection:
		ng, ok := r.readUint32()
		if !ok {
			return false
		}
		nGeoms := int(ng)
		for i := 0; i < nGeoms; i++ {
			subGt, ok := r.readHeader()
			if !ok {
				return false
			}
			switch subGt {
			case wkbPoint:
				if !r.fillPoint(s) {
					return false
				}
				if geoType == GeoTypePoint {
					s.appendPartCoord()
				}
			case wkbLineString:
				if !r.fillLineString(s) {
					return false
				}
				if geoType == GeoTypeLine {
					s.appendPartCoord()
				}
			case wkbPolygon:
				if !r.fillPolygon(s) {
					return false
				}
				if geoType == GeoTypePolygon {
					s.appendPartRing()
				}
			default:
				return false
			}
		}

	default:
		return false
	}

	return true
}

// classifyWkbType returns the GeoType for a WKB base type.
func classifyWkbType(wkbType int) GeoType {
	switch wkbType {
	case wkbPoint, wkbMultiPoint:
		return GeoTypePoint
	case wkbLineString, wkbMultiLineString:
		return GeoTypeLine
	case wkbPolygon, wkbMultiPolygon:
		return GeoTypePolygon
	default:
		return GeoTypeUnknown
	}
}
