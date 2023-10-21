package main

type Message struct {
	//to properly define message
}

type Context struct {
	v_clk []int
}

type Object struct {
	context *Context
	data    string
}

type Node struct {
	channels []chan Message
	rcv_chan chan Message
	tokens   []*Token
	//data
	//backup
}

type Token struct {
	id       int
	phy_node *Node
	//range-start
	//range-end
}
