package Storage

type Queue []string

func (s *Queue) IsEmpty() bool {
	return len(*s) == 0
}

func (s *Queue) GetLen() int {
	return len(*s)
}

func (s *Queue) Push(str string) {
	*s = append(*s, str)
}

func (s *Queue) Pop() (string, bool) {
	if s.IsEmpty() {
		return "", false
	} else {
		element := (*s)[0]
		*s = (*s)[1:]
		return element, true
	}
}
