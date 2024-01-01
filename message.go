package kyu

type Message interface {
	ID() MessageID
	Body() MessageBody
}

type MessageID any
type MessageBody any
