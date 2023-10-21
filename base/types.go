package base

type Message struct {
	//to properly define message
	Command int
	Key     string
	Data    string
}

type Context struct {
	v_clk []int
}

type Object struct {
	context *Context
	data    string
}

func (o *Object) GetData() string {
	return o.data
}

type Node struct {
	id        int
	v_clk     []int
	channels  []chan Message
	rcv_ch    chan Message
	client_ch chan Message //communicates with "frontend" client
	tokens    []*Token
	data      map[string]*Object // key-value data store
	backup    map[string]*Object // backup of key-value data stores
	close_ch  chan struct{}      //to close go channels properly
}

func (n *Node) GetTokens() []*Token {
	return n.tokens
}

func (n *Node) GetID() int {
	return n.id
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
