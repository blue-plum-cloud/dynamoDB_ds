package base

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
	id       int
	v_clk    []int
	channels []chan Message
	rcv_ch   chan Message
	tokens   []*Token
	data     map[string]*Object // key-value data store
	backup   map[string]*Object // backup of key-value data stores
	close_ch chan struct{}      //to close go channels properly
}

func (n *Node) GetTokens() []*Token {
	return n.tokens
}

type Token struct {
	id          int
	phy_node    *Node
	range_start string
	range_end   string
}

func (t *Token) GetStartRange() string {
	return t.range_start
}

func (t *Token) GetEndRange() string {
	return t.range_end
}
