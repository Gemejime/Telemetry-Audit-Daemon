package redaction

type patternKind uint8

const (
	kindNone patternKind = iota
	kindBearer
	kindAuthHeader
	kindSecretKey
	kindAWSKey
	kindPrefixSecret
)

type patternDef struct {
	text string
	kind patternKind
}

var defaultKeyPatterns = []patternDef{
	{text: "bearer ", kind: kindBearer},
	{text: "authorization", kind: kindAuthHeader},
	{text: "api_key", kind: kindSecretKey},
	{text: "apikey", kind: kindSecretKey},
	{text: "x-api-key", kind: kindSecretKey},
	{text: "password", kind: kindSecretKey},
	{text: "passwd", kind: kindSecretKey},
	{text: "secret", kind: kindSecretKey},
	{text: "access_token", kind: kindSecretKey},
	{text: "refresh_token", kind: kindSecretKey},
	{text: "id_token", kind: kindSecretKey},
	{text: "akia", kind: kindAWSKey},
	{text: "ghp_", kind: kindPrefixSecret},
	{text: "gho_", kind: kindPrefixSecret},
	{text: "sk-", kind: kindPrefixSecret},
}

type acNode struct {
	next     [256]int32
	fail     int32
	matchLen int32
	kind     patternKind
}

// automaton is a compiled, zero-allocation Aho-Corasick state machine for ASCII keywords.
type automaton struct {
	nodes []acNode
}

func newAutomaton(defs []patternDef) *automaton {
	a := &automaton{
		nodes: make([]acNode, 1, 64),
	}
	for _, def := range defs {
		cur := int32(0)
		for i := 0; i < len(def.text); i++ {
			c := toLowerASCII(def.text[i])
			nxt := a.nodes[cur].next[c]
			if nxt == 0 {
				nxt = int32(len(a.nodes)) // #nosec G115 -- static small pattern table (<200 nodes)
				a.nodes = append(a.nodes, acNode{})
				a.nodes[cur].next[c] = nxt
			}
			cur = nxt
		}
		a.nodes[cur].matchLen = int32(len(def.text)) // #nosec G115 -- pattern length < 64
		a.nodes[cur].kind = def.kind
	}

	// Build failure links via BFS and pre-fill transitions for O(1) stepping.
	queue := make([]int32, 0, len(a.nodes))
	for c := 0; c < 256; c++ {
		nxt := a.nodes[0].next[c]
		if nxt != 0 {
			a.nodes[nxt].fail = 0
			queue = append(queue, nxt)
		}
	}

	for head := 0; head < len(queue); head++ {
		u := queue[head]
		f := a.nodes[u].fail
		if a.nodes[u].kind == kindNone && a.nodes[f].kind != kindNone {
			a.nodes[u].kind = a.nodes[f].kind
			a.nodes[u].matchLen = a.nodes[f].matchLen
		}
		for c := 0; c < 256; c++ {
			v := a.nodes[u].next[c]
			if v != 0 {
				a.nodes[v].fail = a.nodes[f].next[c]
				queue = append(queue, v)
			} else {
				a.nodes[u].next[c] = a.nodes[f].next[c]
			}
		}
	}
	return a
}

func (a *automaton) step(state int32, b byte) int32 {
	return a.nodes[state].next[toLowerASCII(b)]
}

func toLowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + ('a' - 'A')
	}
	return b
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}

func isAlpha(b byte) bool {
	return (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z')
}

func isAlphaNum(b byte) bool {
	return isAlpha(b) || isDigit(b)
}

func isUpperAlphaNum(b byte) bool {
	return (b >= 'A' && b <= 'Z') || isDigit(b)
}

func isWordBoundaryBefore(src []byte, idx int) bool {
	if idx <= 0 {
		return true
	}
	b := src[idx-1]
	return !isAlphaNum(b) && b != '_'
}

func isNumericBoundaryBefore(src []byte, idx int) bool {
	if idx <= 0 {
		return true
	}
	b := src[idx-1]
	return !isAlphaNum(b) && b != '_' && b != '-' && b != '.'
}

func isWordBoundaryAfter(src []byte, idx int) bool {
	if idx >= len(src) {
		return true
	}
	b := src[idx]
	return !isAlphaNum(b) && b != '_'
}

func isTokenChar(b byte) bool {
	return isAlphaNum(b) || b == '-' || b == '_' || b == '.' || b == '~' || b == '+' || b == '/' || b == '='
}

func isEmailLocalChar(b byte) bool {
	return isAlphaNum(b) || b == '.' || b == '_' || b == '%' || b == '+' || b == '-'
}

func isEmailDomainChar(b byte) bool {
	return isAlphaNum(b) || b == '.' || b == '-'
}

func isBase64URLChar(b byte) bool {
	return isAlphaNum(b) || b == '-' || b == '_'
}

// scanEmail scans backward and forward from an '@' byte at atIdx.
func scanEmail(src []byte, atIdx, minStart int) (start, end int, ok bool) {
	start = atIdx
	for start > minStart && isEmailLocalChar(src[start-1]) {
		start--
	}
	if start == atIdx || !isWordBoundaryBefore(src, start) {
		return 0, 0, false
	}
	// Trim leading dots in local part.
	for start < atIdx && src[start] == '.' {
		start++
	}
	if start == atIdx {
		return 0, 0, false
	}

	end = atIdx + 1
	for end < len(src) && isEmailDomainChar(src[end]) {
		end++
	}
	// Exclude trailing sentence punctuation or hyphens from the domain candidate.
	for end > atIdx+1 && (src[end-1] == '.' || src[end-1] == '-') {
		end--
	}
	lastDot := -1
	for j := atIdx + 1; j < end; j++ {
		if src[j] == '.' {
			lastDot = j
		}
	}
	if lastDot <= atIdx+1 || end-lastDot-1 < 2 {
		return 0, 0, false
	}
	// Verify TLD is alphabetic.
	for j := lastDot + 1; j < end; j++ {
		if !isAlpha(src[j]) {
			return 0, 0, false
		}
	}
	if !isWordBoundaryAfter(src, end) {
		return 0, 0, false
	}
	return start, end, true
}

// scanJWT checks for eyJ<hdr>.eyJ<payload>.<sig> starting at idx.
func scanJWT(src []byte, idx int) (end int, ok bool) {
	if idx+12 >= len(src) || !isWordBoundaryBefore(src, idx) {
		return 0, false
	}
	if src[idx] != 'e' || src[idx+1] != 'y' || src[idx+2] != 'J' {
		return 0, false
	}
	i := idx + 3
	seg1 := 3
	for i < len(src) && isBase64URLChar(src[i]) {
		i++
		seg1++
	}
	if seg1 < 6 || i >= len(src) || src[i] != '.' {
		return 0, false
	}
	i++
	if i+3 >= len(src) || src[i] != 'e' || src[i+1] != 'y' || src[i+2] != 'J' {
		return 0, false
	}
	seg2 := 0
	for i < len(src) && isBase64URLChar(src[i]) {
		i++
		seg2++
	}
	if seg2 < 6 || i >= len(src) || src[i] != '.' {
		return 0, false
	}
	i++
	seg3 := 0
	for i < len(src) && isBase64URLChar(src[i]) {
		i++
		seg3++
	}
	if seg3 < 6 || !isWordBoundaryAfter(src, i) {
		return 0, false
	}
	return i, true
}

// scanSSN matches ddd-dd-dddd with word boundaries.
func scanSSN(src []byte, idx int) (end int, ok bool) {
	if idx+11 > len(src) || !isNumericBoundaryBefore(src, idx) {
		return 0, false
	}
	if !isDigit(src[idx]) || !isDigit(src[idx+1]) || !isDigit(src[idx+2]) ||
		src[idx+3] != '-' ||
		!isDigit(src[idx+4]) || !isDigit(src[idx+5]) ||
		src[idx+6] != '-' ||
		!isDigit(src[idx+7]) || !isDigit(src[idx+8]) || !isDigit(src[idx+9]) || !isDigit(src[idx+10]) {
		return 0, false
	}
	if !isWordBoundaryAfter(src, idx+11) {
		return 0, false
	}
	return idx + 11, true
}

// scanCreditCard matches 13-19 digit PANs (with optional '-' or ' ' group separators)
// and validates the Luhn checksum to prevent false positives on numeric IDs.
func scanCreditCard(src []byte, idx int) (end int, ok bool) {
	if !isNumericBoundaryBefore(src, idx) || !isDigit(src[idx]) {
		return 0, false
	}
	var digits [19]byte
	nDigits := 0
	i := idx
	lastSep := false
	bestEnd := 0
	for i < len(src) && nDigits < 19 {
		b := src[i]
		if isDigit(b) {
			digits[nDigits] = b - '0'
			nDigits++
			lastSep = false
			i++
			continue
		}
		if (b == '-' || b == ' ') && !lastSep && nDigits > 0 && i+1 < len(src) && isDigit(src[i+1]) {
			if nDigits >= 13 && isWordBoundaryAfter(src, i) && luhnValid(digits[:nDigits]) {
				bestEnd = i
			}
			lastSep = true
			i++
			continue
		}
		break
	}
	if nDigits >= 13 && nDigits <= 19 && isWordBoundaryAfter(src, i) && luhnValid(digits[:nDigits]) {
		bestEnd = i
	}
	if bestEnd > 0 {
		return bestEnd, true
	}
	return 0, false
}

func luhnValid(digits []byte) bool {
	sum := 0
	alt := false
	for i := len(digits) - 1; i >= 0; i-- {
		d := int(digits[i])
		if alt {
			d *= 2
			if d > 9 {
				d -= 9
			}
		}
		sum += d
		alt = !alt
	}
	return sum%10 == 0
}

// scanIPv4 matches d.d.d.d where each octet is 0..255.
func scanIPv4(src []byte, idx int) (end int, ok bool) {
	if !isNumericBoundaryBefore(src, idx) || !isDigit(src[idx]) {
		return 0, false
	}
	i := idx
	for octet := 0; octet < 4; octet++ {
		if octet > 0 {
			if i >= len(src) || src[i] != '.' {
				return 0, false
			}
			i++
		}
		if i >= len(src) || !isDigit(src[i]) {
			return 0, false
		}
		val := 0
		digits := 0
		for i < len(src) && isDigit(src[i]) && digits < 3 {
			val = val*10 + int(src[i]-'0')
			digits++
			i++
		}
		if i < len(src) && isDigit(src[i]) {
			return 0, false
		}
		if val > 255 {
			return 0, false
		}
	}
	if !isWordBoundaryAfter(src, i) {
		return 0, false
	}
	return i, true
}

// scanPhone matches formatted phone numbers like +1-555-123-4567, (555) 123-4567, 555-123-4567.
// Requires at least 10 digits and at least one phone formatting character to avoid matching raw integers.
func scanPhone(src []byte, idx int) (end int, ok bool) {
	if !isNumericBoundaryBefore(src, idx) {
		return 0, false
	}
	b0 := src[idx]
	if b0 != '+' && b0 != '(' && !isDigit(b0) {
		return 0, false
	}
	const maxPhoneScanLen = 32
	i := idx
	digits := 0
	hasSep := b0 == '+' || b0 == '('
	lastSep := b0 == '+' || b0 == '('
	if b0 == '+' {
		i++
		if i >= len(src) || !isDigit(src[i]) {
			return 0, false
		}
		lastSep = false
	} else if b0 == '(' {
		i++
	}
	for i < len(src) && digits < 15 && i-idx < maxPhoneScanLen {
		b := src[i]
		if isDigit(b) {
			digits++
			lastSep = false
			i++
			continue
		}
		if b == '-' || b == ' ' || b == '(' || b == ')' || b == '.' {
			if b == '.' {
				// Avoid colliding with IPv4 or version strings; require at least one '-' or '(' or '+'.
				if !hasSep {
					return 0, false
				}
			} else {
				hasSep = true
			}
			// Reject consecutive formatting chars early except ") " / ")-" or " (".
			if lastSep && !(src[i-1] == ')' && (b == ' ' || b == '-')) && !(src[i-1] == ' ' && b == '(') {
				break
			}
			// Do not consume trailing space/punctuation at end of number.
			if i+1 >= len(src) || (!isDigit(src[i+1]) && src[i+1] != '(' && src[i+1] != ' ') {
				if b == ')' && digits >= 3 && i+1 < len(src) && (src[i+1] == ' ' || src[i+1] == '-' || isDigit(src[i+1])) {
					lastSep = true
					i++
					continue
				}
				break
			}
			lastSep = true
			i++
			continue
		}
		break
	}
	if !hasSep || digits < 10 || digits > 15 || !isWordBoundaryAfter(src, i) {
		return 0, false
	}
	return i, true
}

// scanSecretAfterKey extracts the secret value span following a key/prefix match.
func scanSecretAfterKey(src []byte, matchStart, matchEnd int, kind patternKind) (valStart, valEnd int, ok bool) {
	switch kind {
	case kindAWSKey:
		// Must be exact uppercase "AKIA" followed by 16 [A-Z0-9] chars.
		if matchEnd+16 > len(src) || !isWordBoundaryBefore(src, matchStart) {
			return 0, 0, false
		}
		if src[matchStart] != 'A' || src[matchStart+1] != 'K' || src[matchStart+2] != 'I' || src[matchStart+3] != 'A' {
			return 0, 0, false
		}
		for j := matchEnd; j < matchEnd+16; j++ {
			if !isUpperAlphaNum(src[j]) {
				return 0, 0, false
			}
		}
		if !isWordBoundaryAfter(src, matchEnd+16) {
			return 0, 0, false
		}
		return matchStart, matchEnd + 16, true

	case kindPrefixSecret:
		if !isWordBoundaryBefore(src, matchStart) {
			return 0, 0, false
		}
		i := matchEnd
		for i < len(src) && (isAlphaNum(src[i]) || src[i] == '_' || src[i] == '-') {
			i++
		}
		if i-matchEnd < 12 || !isWordBoundaryAfter(src, i) {
			return 0, 0, false
		}
		return matchStart, i, true

	case kindBearer:
		i := matchEnd
		for i < len(src) && src[i] == ' ' {
			i++
		}
		valStart = i
		for i < len(src) && isTokenChar(src[i]) {
			i++
		}
		if i == valStart {
			return 0, 0, false
		}
		return valStart, i, true

	case kindAuthHeader, kindSecretKey:
		i := matchEnd
		// Skip optional closing quote on JSON key name.
		if i < len(src) && (src[i] == '"' || src[i] == '\'') {
			i++
		}
		allowNewline := kind == kindSecretKey
		for i < len(src) && isKeyValueWS(src[i], allowNewline) {
			i++
		}
		if i >= len(src) || (src[i] != ':' && src[i] != '=') {
			return 0, 0, false
		}
		i++
		for i < len(src) && isKeyValueWS(src[i], allowNewline) {
			i++
		}
		if i >= len(src) {
			return 0, 0, false
		}
		// Quoted value (e.g. JSON "password": "secret" or "Authorization": "Bearer xxx")
		if src[i] == '"' || src[i] == '\'' {
			quote := src[i]
			i++
			valStart = i
			for i < len(src) && src[i] != quote {
				if src[i] == '\\' && i+1 < len(src) {
					i += 2
					continue
				}
				i++
			}
			if i == valStart || i >= len(src) {
				return 0, 0, false
			}
			if kind == kindAuthHeader {
				valStart = skipBearerSchemePrefix(src, valStart, i)
				if valStart >= i {
					return 0, 0, false
				}
			}
			return valStart, i, true
		}
		// Unquoted value (e.g. Authorization: Bearer xxx / Basic xxx or api_key=abc123)
		valStart = i
		if kind == kindAuthHeader {
			for i < len(src) && src[i] != '\r' && src[i] != '\n' && src[i] != ',' && src[i] != '"' {
				i++
			}
			for i > valStart && (src[i-1] == ' ' || src[i-1] == '\t') {
				i--
			}
			valStart = skipBearerSchemePrefix(src, valStart, i)
			if i <= valStart {
				return 0, 0, false
			}
			return valStart, i, true
		}
		for i < len(src) && !isKVDelimiter(src[i]) {
			i++
		}
		if i == valStart {
			return 0, 0, false
		}
		return valStart, i, true
	}
	return 0, 0, false
}

func isKeyValueWS(b byte, allowNewline bool) bool {
	if b == ' ' || b == '\t' {
		return true
	}
	return allowNewline && (b == '\r' || b == '\n')
}

func isKVDelimiter(b byte) bool {
	switch b {
	case ' ', '\t', '\r', '\n', ',', ';', '"', '\'', ')', ']', '}':
		return true
	default:
		return false
	}
}

func skipBearerSchemePrefix(src []byte, valStart, valEnd int) int {
	const bearer = "bearer"
	if valEnd-valStart <= len(bearer) {
		return valStart
	}
	for j := 0; j < len(bearer); j++ {
		if toLowerASCII(src[valStart+j]) != bearer[j] {
			return valStart
		}
	}
	if src[valStart+len(bearer)] != ' ' && src[valStart+len(bearer)] != '\t' {
		return valStart
	}
	p := valStart + len(bearer)
	for p < valEnd && (src[p] == ' ' || src[p] == '\t') {
		p++
	}
	return p
}
