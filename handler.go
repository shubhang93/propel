package propel

type Handler interface {
	Handle(record Record)
}
type BatchHandler interface {
	Handle(records Records)
}

type BatchHandlerFunc func(records Records)

func (b BatchHandlerFunc) Handle(records Records) {
	b(records)
}

type HandlerFunc func(record Record)

func (h HandlerFunc) Handle(record Record) {
	h(record)
}
