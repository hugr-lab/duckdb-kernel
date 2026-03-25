/**
 * JSON tree and raw view rendering — platform-agnostic DOM construction.
 */

import type { JsonLine } from './types.js';

function findClosingQuote(s: string, start: number): number {
  let i = start + 1;
  while (i < s.length) {
    if (s[i] === '\\') { i += 2; continue; }
    if (s[i] === '"') return i;
    i++;
  }
  return s.length - 1;
}

export function tokenizeJson(data: any): JsonLine[] {
  let jsonStr: string;
  try {
    jsonStr = JSON.stringify(data, null, 2);
  } catch {
    jsonStr = String(data);
  }

  const rawLines = jsonStr.split('\n');
  const lines: JsonLine[] = [];
  let bracketCounter = 0;

  for (const raw of rawLines) {
    const indent = raw.match(/^(\s*)/)?.[1] ?? '';
    const content = raw.slice(indent.length);
    const tokens: { cls: string; text: string }[] = [];
    let pos = 0;

    while (pos < content.length) {
      const ch = content[pos];

      if (ch === '"') {
        const endQuote = findClosingQuote(content, pos);
        const after = content.slice(endQuote + 1).trimStart();
        const str = content.slice(pos, endQuote + 1);

        if (after.startsWith(':')) {
          tokens.push({ cls: 'hugr-json-key', text: str });
          pos = endQuote + 1;
          const colonMatch = content.slice(pos).match(/^(\s*:\s*)/);
          if (colonMatch) {
            tokens.push({ cls: '', text: colonMatch[1] });
            pos += colonMatch[1].length;
          }
        } else {
          tokens.push({ cls: 'hugr-json-string', text: str });
          pos = endQuote + 1;
        }
      } else if (ch === '{' || ch === '[') {
        tokens.push({ cls: 'hugr-json-bracket', text: ch });
        pos++;
      } else if (ch === '}' || ch === ']') {
        tokens.push({ cls: 'hugr-json-bracket', text: ch });
        pos++;
      } else if (/[0-9\-]/.test(ch)) {
        const numMatch = content.slice(pos).match(/^-?[0-9]+\.?[0-9]*([eE][+-]?[0-9]+)?/);
        if (numMatch) {
          tokens.push({ cls: 'hugr-json-number', text: numMatch[0] });
          pos += numMatch[0].length;
        } else {
          tokens.push({ cls: '', text: ch });
          pos++;
        }
      } else if (content.slice(pos, pos + 4) === 'true') {
        tokens.push({ cls: 'hugr-json-bool', text: 'true' });
        pos += 4;
      } else if (content.slice(pos, pos + 5) === 'false') {
        tokens.push({ cls: 'hugr-json-bool', text: 'false' });
        pos += 5;
      } else if (content.slice(pos, pos + 4) === 'null') {
        tokens.push({ cls: 'hugr-json-null', text: 'null' });
        pos += 4;
      } else {
        tokens.push({ cls: '', text: ch });
        pos++;
      }
    }

    const foldable = /[{\[]$/.test(content.trimEnd().replace(/,\s*$/, ''));
    lines.push({ indent, tokens, foldable });
  }

  // Match bracket pairs for folding
  const stack: { lineIdx: number; id: number }[] = [];
  for (let i = 0; i < lines.length; i++) {
    const content = lines[i].tokens.map(t => t.text).join('').trim();
    if (/[{\[]\s*$/.test(content)) {
      const id = bracketCounter++;
      lines[i].bracketId = id;
      stack.push({ lineIdx: i, id });
    }
    if (/^[}\]]/.test(content) && stack.length > 0) {
      const open = stack.pop()!;
      lines[open.lineIdx].foldEnd = i;
      lines[i].bracketId = open.id;
    }
  }

  return lines;
}

function toggleFold(
  btn: HTMLElement,
  startLine: number,
  endLine: number,
  gutterLines: HTMLElement[],
  codeLines: HTMLElement[],
  lines: JsonLine[],
): void {
  const isCollapsed = btn.textContent === '\u25B6';

  if (isCollapsed) {
    btn.textContent = '\u25BC';
    for (let i = startLine + 1; i <= endLine; i++) {
      gutterLines[i].style.display = '';
      codeLines[i].style.display = '';
    }
    const placeholder = codeLines[startLine].querySelector('.hugr-json-fold-placeholder');
    if (placeholder) placeholder.remove();
  } else {
    btn.textContent = '\u25B6';
    for (let i = startLine + 1; i <= endLine; i++) {
      gutterLines[i].style.display = 'none';
      codeLines[i].style.display = 'none';
    }
    const hiddenCount = endLine - startLine - 1;
    const placeholder = document.createElement('span');
    placeholder.className = 'hugr-json-fold-placeholder';
    placeholder.textContent = ` ... ${hiddenCount} lines `;
    placeholder.addEventListener('click', () => {
      toggleFold(btn, startLine, endLine, gutterLines, codeLines, lines);
    });
    codeLines[startLine].appendChild(placeholder);
  }
}

/** Build a syntax-highlighted raw JSON view with line numbers, folding, and bracket matching. */
export function buildJsonRawView(data: any, container: HTMLElement): void {
  const lines = tokenizeJson(data);

  const gutter = document.createElement('div');
  gutter.className = 'hugr-json-gutter';

  const code = document.createElement('div');
  code.className = 'hugr-json-code';

  const gutterLines: HTMLElement[] = [];
  const codeLines: HTMLElement[] = [];
  const bracketSpans = new Map<number, HTMLElement[]>();

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];

    const gutterLine = document.createElement('span');
    gutterLine.className = 'hugr-json-gutter-line';
    gutterLine.textContent = String(i + 1);
    gutter.appendChild(gutterLine);
    gutterLines.push(gutterLine);

    const codeLine = document.createElement('span');
    codeLine.className = 'hugr-json-code-line';

    if (line.foldable && line.foldEnd != null) {
      const foldBtn = document.createElement('span');
      foldBtn.className = 'hugr-json-fold-btn';
      foldBtn.textContent = '\u25BC';
      const foldStart = i;
      const foldEndIdx = line.foldEnd;
      foldBtn.addEventListener('click', () => {
        toggleFold(foldBtn, foldStart, foldEndIdx, gutterLines, codeLines, lines);
      });
      codeLine.appendChild(foldBtn);
    } else {
      const spacer = document.createElement('span');
      spacer.className = 'hugr-json-fold-btn';
      spacer.textContent = ' ';
      codeLine.appendChild(spacer);
    }

    if (line.indent) {
      codeLine.appendChild(document.createTextNode(line.indent));
    }

    for (const token of line.tokens) {
      const span = document.createElement('span');
      if (token.cls) span.className = token.cls;
      span.textContent = token.text;

      if (token.cls === 'hugr-json-bracket' && line.bracketId != null) {
        let arr = bracketSpans.get(line.bracketId);
        if (!arr) { arr = []; bracketSpans.set(line.bracketId, arr); }
        arr.push(span);
      }

      codeLine.appendChild(span);
    }

    code.appendChild(codeLine);
    codeLines.push(codeLine);
  }

  // Bracket matching on hover
  let currentHighlight: HTMLElement[] = [];
  code.addEventListener('mouseover', (e) => {
    const target = e.target as HTMLElement;
    if (!target.classList.contains('hugr-json-bracket')) return;

    for (const el of currentHighlight) el.classList.remove('hugr-json-bracket-highlight');
    currentHighlight = [];

    for (const [, spans] of bracketSpans) {
      if (spans.includes(target)) {
        for (const s of spans) s.classList.add('hugr-json-bracket-highlight');
        currentHighlight = spans;
        break;
      }
    }
  });

  code.addEventListener('mouseleave', () => {
    for (const el of currentHighlight) el.classList.remove('hugr-json-bracket-highlight');
    currentHighlight = [];
  });

  container.appendChild(gutter);
  container.appendChild(code);
}

/** Recursively build a collapsible JSON tree. */
export function buildJsonTree(data: any, parent: HTMLElement, expanded: boolean): void {
  if (data === null || data === undefined) {
    const val = document.createElement('span');
    val.className = 'hugr-json-null';
    val.textContent = 'null';
    parent.appendChild(val);
    return;
  }

  if (Array.isArray(data)) {
    if (data.length === 0) {
      const val = document.createElement('span');
      val.className = 'hugr-json-bracket';
      val.textContent = '[]';
      parent.appendChild(val);
      return;
    }
    buildCollapsible(data, parent, expanded, true);
    return;
  }

  if (typeof data === 'object') {
    const keys = Object.keys(data);
    if (keys.length === 0) {
      const val = document.createElement('span');
      val.className = 'hugr-json-bracket';
      val.textContent = '{}';
      parent.appendChild(val);
      return;
    }
    buildCollapsible(data, parent, expanded, false);
    return;
  }

  // Primitive value
  const val = document.createElement('span');
  if (typeof data === 'string') {
    val.className = 'hugr-json-string';
    val.textContent = JSON.stringify(data);
  } else if (typeof data === 'number') {
    val.className = 'hugr-json-number';
    val.textContent = String(data);
  } else if (typeof data === 'boolean') {
    val.className = 'hugr-json-bool';
    val.textContent = String(data);
  } else {
    val.textContent = String(data);
  }
  parent.appendChild(val);
}

/** Build a collapsible object/array node. */
function buildCollapsible(data: any, parent: HTMLElement, expanded: boolean, isArray: boolean): void {
  const count = isArray ? data.length : Object.keys(data).length;

  const row = document.createElement('div');
  row.className = 'hugr-json-row';

  const toggle = document.createElement('span');
  toggle.className = 'hugr-json-toggle';
  toggle.textContent = expanded ? '\u25BC' : '\u25B6';
  row.appendChild(toggle);

  const summary = document.createElement('span');
  summary.className = 'hugr-json-summary';
  summary.textContent = isArray ? `Array(${count})` : `{${count} keys}`;
  row.appendChild(summary);

  parent.appendChild(row);

  const children = document.createElement('div');
  children.className = 'hugr-json-children';
  children.style.display = expanded ? '' : 'none';

  if (isArray) {
    for (let i = 0; i < data.length; i++) {
      const entry = document.createElement('div');
      entry.className = 'hugr-json-entry';
      const key = document.createElement('span');
      key.className = 'hugr-json-index';
      key.textContent = `${i}: `;
      entry.appendChild(key);
      buildJsonTree(data[i], entry, false);
      children.appendChild(entry);
    }
  } else {
    for (const [k, v] of Object.entries(data)) {
      const entry = document.createElement('div');
      entry.className = 'hugr-json-entry';
      const key = document.createElement('span');
      key.className = 'hugr-json-key';
      key.textContent = `${k}: `;
      entry.appendChild(key);
      buildJsonTree(v, entry, false);
      children.appendChild(entry);
    }
  }

  parent.appendChild(children);

  const doToggle = () => {
    const isOpen = children.style.display !== 'none';
    children.style.display = isOpen ? 'none' : '';
    toggle.textContent = isOpen ? '\u25B6' : '\u25BC';
  };
  toggle.addEventListener('click', doToggle);
  summary.addEventListener('click', doToggle);
}
