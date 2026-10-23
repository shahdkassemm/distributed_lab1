// Package rpc is an intentionally incomplete RMI assignment template.
package rpc

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"sync"
	"time"
	"go/token"
	"strings"
	"unicode"
)

const (
	maxArguments       = 8
	maxArgumentBytes   = 1 << 20
	maxRequestBytes    = 4 << 20
	defaultCallTimeout = 5 * time.Second
)

type request struct {
	Method string            `json:"method"`
	Args   []json.RawMessage `json:"args"`
}

type response struct {
	OK    bool            `json:"ok"`
	Value json.RawMessage `json:"value,omitempty"`
	Error string          `json:"error,omitempty"`
}

// Server exposes exported methods of a receiver over TCP.
type Server struct {
	receiver   any
	listener   net.Listener
	stopped    chan struct{}
	acceptDone chan struct{}
	workers    sync.WaitGroup
	mu         sync.Mutex
}

// NewServer creates a server for a non-nil pointer receiver.
func NewServer(receiver any) (*Server, error) {
	if receiver == nil {
		return nil, errors.New("receiver cannot be nil")
	}
	value := reflect.ValueOf(receiver)
	if value.Kind() != reflect.Pointer || value.IsNil() {
		return nil, errors.New("receiver must be a non-nil pointer")
	}
	return &Server{receiver: receiver}, nil
}

// TODO 1: Create the TCP listener, reject duplicate starts, save lifecycle state,
// and launch the accept loop without blocking the caller.
// Start begins accepting remote calls at address. Use port 0 to request an available port.
// TODO 1: Start el Server

func (s *Server) Start(address string) error {
    // De function esmaha Start, btakhod address el Server
    // w btraga3 error law 7asalet ay moshkela

    s.mu.Lock()
    // Bne3mel Lock 3shan nmen3 aktar mn Goroutine
    // enha t3adel fel Server fel nafs el wa2t

    defer s.mu.Unlock()
    // Defer ya3ny nafaz el Unlock lama el function tekhlas
    // 3shan manenseesh neftah el Lock

    if s.listener != nil {
        // Bnet2aked law el Server already sh8al aw lesa by2fel

        return errors.New("server is already running or stopping")
        // Law sh8al, bneraga3 Error 3shan manesh8alosh marrtein
    }

    listener, err := net.Listen("tcp", address)
    // Benefte7 TCP Listener 3ala el address elly etb3at
    // Listener da byesta2bel connections mn el Clients
    // err byeshil ay error momken ye7sal

    if err != nil {
        // Law 7asal error fel Listen

        return fmt.Errorf("start server: %w", err)
        // Bneraga3 el error ma3 message betwada7
        // en el moshkela 7asalet w e7na bensh8al el Server
    }

    s.listener = listener
    // Ben5azen el Listener gowa el Server
    // 3shan ne2dar nesta5demo ba3d keda

    s.stopped = make(chan struct{})
    // Ben3mel Channel gedida
    // Han2felha lama ne7eb nwa2af el Server

    s.acceptDone = make(chan struct{})
    // Channel 3shan ne3raf emta el Accept Loop khalaset

    go s.acceptLoop(listener, s.stopped, s.acceptDone)
    // "go" betsh8al el Accept Loop fe Goroutine mosta2ela
    // 3shan el Server yesta2bel Clients
    // mn 8er ma Start testana el Loop tekhlas

    return nil
    // Nil ya3ny mafish error w el Server etsh8al benga7
}


// Helper Function: acceptLoop

func (s *Server) acceptLoop(
    listener net.Listener,
    // El Listener elly hayesta2bel connections

    stopped <-chan struct{},
    // Channel benesta2bel menha signal en el Server wa2af

    done chan<- struct{},
    // Channel beneb3at beha signal en el Loop khalaset
) {
    defer close(done)
    // Lama el Loop tekhlas, han2fel done
    // 3shan Stop te3raf en el Loop khalaset

    for {
        // Infinite Loop 3shan el Server yefdal
        // mestany Clients gedad

        connection, err := listener.Accept()
        // Benestana Client ye3mel Connection
        // Accept betestana le7ad ma Client yetesel
        // aw el Listener yet2efel

        if err != nil {
            // Law 7asal Error, masalan el Listener et2efel

            return
            // Benwa2af el Accept Loop
        }

        s.mu.Lock()
        // Ben3mel Lock 3shan ne7mi 7alet el Server
        // w nensa2 ma3 Stop()

        select {
        // Select betesta5dam ma3 Channels

        case <-stopped:
            // Law stopped et2efel, ya3ny Stop() etnadet

            s.mu.Unlock()
            // Benefok el Lock

            _ = connection.Close()
            // Ben2fel el Connection el gedida
            // 3shan el Server ba2a bywa2af

            return
            // Benkhrog mn el Loop

        default:
            // Law mafish Stop signal
            // Hankamel 3ady
        }

        s.workers.Add(1)
        // Ben2ol lel WaitGroup en fe Worker gedid
        // 3shan Stop() testana kol el Workers yekhlasou

        s.mu.Unlock()
        // Benefok el Lock 3shan ba2y el Goroutines
        // te2dar testa5dem el Server

        go func() {
            // Benesh8al Goroutine gedida le kol Client
            // 3shan ne2dar net3amel ma3 aktar mn Client

            defer s.workers.Done()
            // Lama el Worker yekhlas, bne2alel el counter
            // bta3 el WaitGroup

            s.handle(connection)
            // De el Function elly bet2ra Request el Client
            // w tenafez el Method w teraga3 el Response
        }()
    }
}
// TODO 2: Return the listener address safely while allowing port 0 discovery.
// Address returns the address assigned to the running server.
// TODO 2: Return el Server Address

func (s *Server) Address() string {
    // De Method betgeb el Address bta3 el Server
    // w bteraga3o ka String

    s.mu.Lock()
    // Ben3mel Lock 3shan ne7mi el shared data
    // w man5aleesh Goroutine t8ayar el Listener
    // w e7na ben2ra el Address

    defer s.mu.Unlock()
    // Lama el Function tekhlas, el Lock hayetfek automatic
    // 7atta law 3amalna return badry

    if s.listener == nil {
        // Benet2aked law mafish Listener
        // ya3ny el Server mesh sh8al

        return ""
        // Benraga3 Empty String 3shan mafish Address
    }

    select {
        // Bencheck el stopped Channel
        // 3shan ne3raf el Server bywa2af wala la

    case <-s.stopped:
        // Law el Channel et2afal
        // ya3ny el Server wa2af aw bywa2af

        return ""
        // Benraga3 Empty String 3shan el Address
        // mab2ash available

    default:
        // Law mafish Stop Signal
        // ya3ny el Server lesa sh8al

        return s.listener.Addr().String()
        // Addr() betgeb el IP w el Port bta3 el Listener
        // String() bet7awel el Address le String
        // Masalan: 127.0.0.1:8080
    }
}
// TODO 3: Close the listener, signal the accept loop, wait for its workers,
// and leave the server in a state where Start can be called again.
// Stop stops accepting new connections and waits for active handlers to finish.
// TODO 3: Stop el Server safely

func (s *Server) Stop() error {
    // De Method mas2oola 3an enha twa2af el Server
    // w testana kol el Active Clients yekhalaso

    s.mu.Lock()
    // Ben3mel Lock 3shan ne7mi el Shared Server State
    // w mankhaleesh aktar mn Goroutine t3adelo fel nafs el wa2t

    if s.listener == nil {
        // Bencheck law mafish Listener
        // ya3ny el Server already stopped aw lesa matesh8alsh

        s.mu.Unlock()
        // Lazem nefok el Lock abl ma ne3mel return

        return nil
        // Mafish Error 3shan el Server already stopped
    }

    listener := s.listener
    // Ben7faz reference lel Listener el 7aly
    // 3shan nesta5demo fel Close

    done := s.acceptDone
    // Ben7faz el Channel elly hate2olna
    // emta el Accept Loop tekhlas

    select {
        // Bencheck hal el stopped Channel et2eflet wala la

    case <-s.stopped:
        // Law el Channel already closed
        // ya3ny Stop etnadet abl keda
        // mesh me7tageen ne2felha tany

    default:
        // Law el Channel lesa maet2efletsh

        close(s.stopped)
        // Ben2fel el Channel 3shan neb3at Stop Signal
        // lel Accept Loop

        _ = listener.Close()
        // Ben2fel el TCP Listener
        // 3shan Accept() tebatal testana Connections gedida
        // el _ ya3ny mesh benesta5dem el Error elly Close bteraga3o
    }

    s.mu.Unlock()
    // Benefok el Lock 3shan el Goroutines
    // te2dar tekamel shoghlaha w tekhlas

    <-done
    // Benestana le7ad ma el Accept Loop tekhlas
    // lama done tet2efel, el entezar da hayekhlas

    s.workers.Wait()
    // Benestana kol el Worker Goroutines
    // elly sh8ala ma3 el Clients tekhlas
    // 3shan manwa2afsh el Server w fe Requests lesa btet3aleg

    s.mu.Lock()
    // Ben3mel Lock tany 3shan ne3adel
    // el Server State be aman

    if s.listener == listener {
        // Benet2aked en el Listener el 7aly
        // howa nafs el Listener elly kona benwa2afo

        s.listener = nil
        // Benmsa7 el Reference lel Listener
        // 3shan el Server yeb2a considered stopped

        s.stopped = nil
        // Benmsa7 el Reference lel Stop Channel

        s.acceptDone = nil
        // Benmsa7 el Reference lel Accept Done Channel
        // 3shan el Server yeb2a ready lel Start tany
    }

    s.mu.Unlock()
    // Benefok el Lock ba3d ma khalasna el Cleanup

    return nil
    // Benraga3 nil ya3ny Stop khalaset
    // mn 8er ma nrega3 Error
}
// TODO 4: Study this starter request handler, then complete and test the protocol
// contract. Add tests for malformed JSON, oversized input, remote errors, panics,
// and successful calls, and make sure one bad request cannot crash the server or
// leave a connection hanging. Connect this handler to your completed Start and
// Stop lifecycle so each accepted connection is processed by a worker safely.
// handle reads one request, invokes it, and writes one response.
// TODO 4: Handle el Requests elly gaya mn el Clients

func (s *Server) handle(connection net.Conn) {
    // De Method btet3amel ma3 Connection wa7da mn Client
    // Betesta2bel el Request w teraga3 el Response

    defer connection.Close()
    // Lama el Function tekhlas, el Connection hatet2efel
    // 3shan manseebsh Connections maftoo7a

    _ = connection.SetDeadline(time.Now().Add(10 * time.Second))
    // Ben7aded Timeout b 10 seconds
    // Law el Read aw Write et2akhar, el Operation momken tefail
    // 3shan el Server mayefdalsh mestany lel abad
    // _ ya3ny mesh benesta5dem el Error elly bterga3

    encoder := json.NewEncoder(connection)
    // Ben3mel JSON Encoder
    // 3shan neb3at el Response lel Client ka JSON

    defer func() {
        // Anonymous Function hatetnafez lama handle tekhlas
        // W btet3amel ma3 ay Panic momken te7sal

        if recovered := recover(); recovered != nil {
            // recover() betmasak el Panic law 7asalet
            // 3shan el Goroutine matwa2afsh besababha

            _ = encoder.Encode(response{
                // Benb3at Response feha Error lel Client

                Error: fmt.Sprintf(
                    "remote method panicked: %v",
                    recovered,
                ),
                // Sprintf bet7awel el Panic le Error Message
                // %v bet7ot el Value bta3et el Panic
            })
        }
    }()

    decoder := json.NewDecoder(
        // Ben3mel JSON Decoder
        // 3shan ne2ra el Request elly gaya mn el Client

        bufio.NewReader(
            // Buffered Reader bysa3edna ne2ra el Data

            io.LimitReader(connection, maxRequestBytes+1),
            // Ben7aded maximum 3adad Bytes momken ne2raha
            // 3shan el Client mayeb3atsh Data kbira awy
            // +1 3shan ne2dar nel7az en el Limit et3adda
        ),
    )

    var req request
    // Ben3araf Variable esmo req mn Type request
    // Da hayeshil el Method Name wel Arguments

    if err := decoder.Decode(&req); err != nil {
        // Decode bet7awel el JSON elly gaya le request Struct
        // &req ya3ny benb3at Address el Variable
        // 3shan Decode te2dar temla el Data gowah

        // Law el JSON invalid aw 7asal Error fel Reading

        _ = encoder.Encode(response{
            Error: fmt.Sprintf("invalid request: %v", err),
        })
        // Benb3at Response lel Client en el Request 8alat

        return
        // Benwa2af el Function 3shan mankamelsh Request 8alat
    }

    if err := validateRequest(req); err != nil {
        // Benesta5dem validateRequest
        // 3shan net2aked en el Request sa7
        // Zay Method Name wel Arguments wel Size

        _ = encoder.Encode(response{
            Error: err.Error(),
        })
        // Law el Request invalid
        // Benb3at el Error Message lel Client

        return
        // Mankamelsh Execution law el Request 8alat
    }

    result, err := s.invoke(req)
    // invoke() betdawar 3ala el Method elly el Client talabha
    // W betnafezha bel Arguments
    // result = el Natiga
    // err = ay Error 7asal fel Execution

    if err != nil {
        // Law 7asal Error fel invoke

        _ = encoder.Encode(response{
            Error: err.Error(),
        })
        // Benb3at el Error lel Client

        return
        // Benwa2af el Function
    }

    payload, err := json.Marshal(result)
    // Marshal bet7awel el Result le JSON Bytes
    // 3shan ne2dar neb3atha lel Client
    // payload hayeshil el Result ba3d el Conversion

    if err != nil {
        // Law el Result mayenfa3sh yet7awel le JSON

        _ = encoder.Encode(response{
            Error: fmt.Sprintf("encode result: %v", err),
        })
        // Benb3at Error en el Result failed to encode

        return
        // Benwa2af el Function
    }

    _ = encoder.Encode(response{
        // Benb3at el Final Response lel Client

        OK: true,
        // True ya3ny el Request etnafez benga7

        Value: payload,
        // El Result elly et7awel le JSON
    })

    // Keda el Client hayestelem el Response
    // W lama el Function tekhlas, el Connection hatet2efel
}

// TODO 5: Reject empty or unsafe method names, too many arguments, arguments
// larger than maxArgumentBytes, and requests larger than maxRequestBytes.
// validateRequest rejects malformed or abusive requests before reflection runs.
// TODO 5: Check en el Request sa7 w aman abl ma netfezo

func validateRequest(req request) error {
    // De Function betakhod Request mn el Client
    // W betrag3 Error law el Request feeh moshkela
    // Aw nil law kol 7aga sa7

    if strings.TrimSpace(req.Method) == "" {
        // TrimSpace betesheel el Spaces mn awal w a5er el String
        // Bencheck law el Method Name fady aw kollo Spaces

        return errors.New("method cannot be empty")
        // Benraga3 Error 3shan el Method Name mayenfa3sh yeb2a fady
    }

    if strings.IndexFunc(req.Method, func(r rune) bool {
        // IndexFunc betdawar 3ala awel Character
        // Bey7a2a2 el Condition elly gowwa el Function
        // rune ya3ny Unicode Character

        return unicode.IsSpace(r) || unicode.IsControl(r)
        // IsSpace betcheck law el Character Space
        // IsControl betcheck law Tab aw Newline aw Control Character
        // || ya3ny OR: ay Condition fehom true

    }) >= 0 {
        // Law IndexFunc rag3et 0 aw aktar
        // Ya3ny la2et Character mamnoo3

        return errors.New(
            "method contains whitespace or control characters",
        )
        // Benrefod el Request law feeh Spaces aw Control Characters
    }

    if !token.IsIdentifier(req.Method) ||
        !unicode.IsUpper([]rune(req.Method)[0]) {
        // IsIdentifier betcheck en el Method Name
        // Yenfa3 yeb2a Identifier sa7 fe Go
        // ! ya3ny NOT
        // []rune bet7awel el String le Unicode Characters
        // [0] ya3ny awel Character
        // IsUpper betcheck en awel 7arf Capital
        // || ya3ny law ay Condition 8alat hanrefod el Method

        return fmt.Errorf(
            "unsafe method name %q",
            req.Method,
        )
        // Benraga3 Error feeh esm el Method el 8alat
        // %q bet3red el String gowwa Quotation Marks
    }

    if len(req.Args) > maxArguments {
        // len betgeeb 3adad el Arguments
        // Bencheck en el Client mab3atsh Arguments aktar mn el Limit

        return fmt.Errorf(
            "too many arguments: maximum is %d",
            maxArguments,
        )
        // Benraga3 Error law 3adad el Arguments kbeer
        // %d bet7ot Integer fel Error Message
    }

    size := len(req.Method) + len(`{"method":"","args":[]}`)
    // Ben7seb Size ta2reeby lel Request bel Bytes
    // len(req.Method) betgeeb 7agm el Method Name bel Bytes
    // W benzawed 7agm el JSON Structure el asasy
    // := ya3ny Declaration w Assignment lel Variable

    for _, arg := range req.Args {
        // Loop bet3addy 3ala kol Argument
        // _ ya3ny mesh me7tageen el Index
        // arg howa el Argument el 7aly

        if len(arg) > maxArgumentBytes {
            // Bencheck en 7agm el Argument
            // Mesh akbar mn el Maximum Allowed Size

            return fmt.Errorf(
                "argument larger than %d bytes",
                maxArgumentBytes,
            )
            // Benraga3 Error law el Argument kbeer awy
        }

        size += len(arg) + 1
        // Benzawed 7agm kol Argument 3ala Total Size
        // +1 3shan nefakker fe el Separator ben el Arguments
        // += ya3ny size = size + ...
    }

    if size > maxRequestBytes {
        // Bencheck en Total Request Size
        // Mesh akbar mn el Maximum Allowed Size

        return fmt.Errorf(
            "request larger than %d bytes",
            maxRequestBytes,
        )
        // Benraga3 Error law el Request kolo kbeer awy
    }

    return nil
    // Law wasalna hena ya3ny kol el Validation Checks 3addet
    // Mafish Error w el Request accepted
}

// TODO 6: Study and test this starter reflection bridge. Your required work is to
// support and test exported methods with zero arguments, typed arguments, no
// return value, one return value, and a final error return; unknown methods,
// wrong argument counts, invalid JSON arguments, returned errors, and panics must
// become clear errors instead of crashing the server. Extend the bridge only when
// your chosen assignment protocol requires another result or argument rule.
// invoke finds a method, converts JSON arguments to its parameter types, and calls it.
// TODO 6: Execute el Method elly el Client talabha
// Benesta5dem Reflection 3shan ne3raf w nenafez el Method bel esm

func (s *Server) invoke(req request) (any, error) {
    // De Method betakhod Request
    // W bteraga3 Result mn ay Type (any) w Error

    method := reflect.ValueOf(s.receiver).MethodByName(req.Method)
    // reflect.ValueOf(s.receiver) betgeb Reflection Value lel Receiver
    // MethodByName betdawar 3ala Method bel esm elly fel Request
    // Masalan law req.Method = "Add", hatdawar 3ala Add
    // method hayeshil el Method elly la2enaha

    if !method.IsValid() {
        // IsValid betcheck law el Method mawgooda
        // ! ya3ny NOT
        // Law mesh mawgooda, el Condition hatb2a True

        return nil, fmt.Errorf(
            "unknown method %q",
            req.Method,
        )
        // Benraga3 Error en el Method mesh mawgooda
        // %q bet7ot el Method Name ben Quotation Marks
    }

    typeOfMethod := method.Type()
    // Bengeb Type Information bta3 el Method
    // Zay 3adad el Inputs w Outputs w Types beta3hom

    errorType := reflect.TypeOf((*error)(nil)).Elem()
    // Bengeb Reflection Type bta3 el error Interface
    // (*error)(nil) bet3mel nil Pointer le error Interface
    // Elem() betgeb el Type elly el Pointer byeshawer 3aleh
    // Hanesta5demo 3shan necheck el Error Return Type

    if typeOfMethod.NumOut() > 2 ||
        (typeOfMethod.NumOut() == 2 &&
            !typeOfMethod.Out(1).Implements(errorType)) {
        // NumOut() betgeb 3adad el Values elly el Method bteraga3ha
        // Law aktar mn 2, hanrefod el Method
        // Law bteraga3 2 Values, el Tany lazm yeb2a Error
        // Out(1) ya3ny el Second Return Type
        // Implements betcheck law el Type byimplement el error Interface
        // && ya3ny AND, || ya3ny OR

        return nil, fmt.Errorf(
            "unsupported return signature for method %q",
            req.Method,
        )
        // Benraga3 Error law el Return Signature mesh supported
    }

    if typeOfMethod.NumIn() != len(req.Args) {
        // NumIn() betgeb 3adad el Parameters elly el Method mestanyaha
        // len(req.Args) betgeb 3adad el Arguments elly el Client ba3atha
        // != ya3ny Not Equal

        return nil, fmt.Errorf(
            "method %q expects %d arguments, got %d",
            req.Method,
            typeOfMethod.NumIn(),
            len(req.Args),
        )
        // Benraga3 Error law el Argument Count mesh sa7
        // Masalan Add mestanya 2 Arguments bas el Client ba3at 3
    }

    arguments := make([]reflect.Value, len(req.Args))
    // Ben3mel Slice gedida mn reflect.Value
    // 7agmha adad el Arguments elly gaya mn el Client
    // Han7ot feeha el Arguments ba3d ma ne7awelha lel Types el sa7

    for index, raw := range req.Args {
        // Loop 3ala kol Argument
        // index = Raqm el Argument
        // raw = el Argument bel JSON Format

        argument := reflect.New(typeOfMethod.In(index))
        // In(index) betgeb el Type elly el Method mestanyah
        // reflect.New bet3mel Pointer le Value gedida mn el Type da
        // 3shan ne2dar nemlaha bel Data elly gaya mn el Client

        if err := json.Unmarshal(raw, argument.Interface()); err != nil {
            // Unmarshal bet7awel JSON Data le Go Value
            // argument.Interface() betkhaleena nesta5dem el Value
            // ka Go Interface 3ady
            // Law el JSON mesh compatible ma3 el Type, haye7sal Error

            return nil, fmt.Errorf(
                "argument %d: %v",
                index,
                err,
            )
            // Benraga3 Error w n7aded anhy Argument feha el Moshkela
        }

        arguments[index] = argument.Elem()
        // Elem() betgeb el Value elly gowwa el Pointer
        // Ben7ot el Value fel Arguments Slice
        // 3shan teb2a gahza lel Method Call
    }

    outputs, err := callMethodSafely(method, arguments)
    // Beneb3at el Method wel Arguments le Helper Function
    // 3shan tenafez el Method safely
    // outputs = el Results
    // err = ay Error aw Panic etmasaket

    if err != nil {
        // Law 7asal Error fel Execution

        return nil, err
        // Benraga3 el Error mn 8er ma nekamel
    }

    if len(outputs) == 0 {
        // Law el Method mabteraga3sh ay Values

        return nil, nil
        // Mafish Result w mafish Error
    }

    if last := outputs[len(outputs)-1]; last.Type().Implements(errorType) {
        // Bengeb a5er Value fel Outputs
        // len(outputs)-1 ya3ny Index a5er Element
        // Bencheck law el Value de Type beta3ha implements error

        if !isNilValue(last) {
            // Bencheck law el Error mesh nil
            // Ya3ny fe Error fe3lan

            return nil, last.Interface().(error)
            // Ben7awel el Value le error Interface
            // W benraga3 el Error lel Caller
        }

        outputs = outputs[:len(outputs)-1]
        // Law el Error nil, bensheelha mn el Outputs
        // 3shan nefadal bel Actual Result bas
        // [:len(outputs)-1] ya3ny kol el Elements ma3ada el A5er
    }

    if len(outputs) == 0 {
        // Law mafish Results ba3d ma shelna el Error

        return nil, nil
        // Benraga3 nil Result w nil Error
    }

    return outputs[0].Interface(), nil
    // Benraga3 awel Result mn el Method
    // Interface() bet7awel reflect.Value le Go Value 3adya
    // nil ya3ny el Execution nag7 mn 8er Error
}
// Helper Function: Check law el Value nil

func isNilValue(value reflect.Value) bool {
    // Betakhod Reflection Value
    // W bteraga3 True law el Value nil, False law la2

    switch value.Kind() {
        // Kind() betgeb el Basic Kind bta3 el Value
        // Zay Pointer, Slice, Map, Interface

    case reflect.Chan, reflect.Func, reflect.Interface,
        reflect.Map, reflect.Pointer, reflect.Slice:
        // Dol el Kinds elly momken testa5dem ma3aha IsNil()
        // Chan = Channel
        // Func = Function
        // Interface = Interface Value
        // Map = Key-Value Collection
        // Pointer = Reference le Value
        // Slice = Dynamic Sequence

        return value.IsNil()
        // Betcheck law el Value nil wla la2
        // True law nil, False law mesh nil

    default:
        // Law el Value Kind mesh mn el Kinds elly fo2
        // Zay int aw bool

        return false
        // Benraga3 False 3shan IsNil mesh supported lel Kind da
    }
}
// Helper Function: Execute el Method safely
// Law el Method 3amlet Panic, ben7awelha le Error badal ma el Server yecrash
func callMethodSafely(
    method reflect.Value,
    arguments []reflect.Value,
) (outputs []reflect.Value, err error) {
    // method = el Method elly hanenafezha
    // arguments = el Inputs elly hatetb3at lel Method
    // outputs = el Values elly el Method hateraga3ha
    // err = el Error law 7asalet moshkela

    defer func() {
        // Defer betetnfez abl ma el Function tekhlas
        if recovered := recover(); recovered != nil {
            // recover betmasak el Panic fe nafs el Goroutine
            err = fmt.Errorf("remote method panicked: %v", recovered)
            // Ben7awel el Panic le Error Message
        }
    }()

    return method.Call(arguments), nil
    // Call betenafez el Method bel Arguments
    // W bteraga3 el Outputs, w nil law mafish Error
}

// Client Struct: by7faz el Server Address wel Timeout
type Client struct {
    Address string
    // El IP wel Port bta3 el Server

    Timeout time.Duration
    // A2sa wa2t lel Remote Call
}

// NewClient: create Client gedid lel Server Address
func NewClient(address string) (*Client, error) {
    // Benakhod el Address w bneraga3 Client Pointer aw Error
    if address == "" {
        // Law el Address fady, haneraga3 Error
        return nil, errors.New("address cannot be empty")
    }

    return &Client{
        Address: address,
        // Ben7ot el Address elly etb3at
        Timeout: defaultCallTimeout,
        // Ben7ot Default Timeout = 5 Seconds
    }, nil
    // Client et3amal benga7, w mafish Error
}

// TODO 7: Open one connection, apply a deadline, encode the request, read one
// response, return remote errors, and decode the response into out.
// Call invokes method and decodes its result into out. Pass nil for no result.
// TODO 7: Send Request mn el Client lel Server
// W nesta2bel el Response w ne7faz el Result

func (c *Client) Call(method string, out any, args ...any) error {
    // De Method taba3 el Client esmaha Call
    // method = esm el Function elly 3ayzeen nenafezha
    // out = el Variable elly han7ot feeha el Result
    // args ...any = ay 3adad mn el Arguments b ay Data Type
    // Bteraga3 error law 7asalet moshkela aw nil law nag7et

    req := request{
        // Ben3mel Request gedid 3shan neb3ato lel Server

        Method: method,
        // Ben7ot esm el Method elly el Client 3ayez yenafezha
        // Masalan "Add" aw "Ping"

        Args: make([]json.RawMessage, len(args)),
        // Ben3mel Slice le JSON Arguments
        // len(args) = 3adad el Arguments elly el Client ba3atha
        // make() bet3mel Slice bel Size elly e7na me7tageno
    }

    for i, arg := range args {
        // Loop 3ala kol Argument
        // i = Index bta3 el Argument
        // arg = el Argument el 7aly

        data, err := json.Marshal(arg)
        // Marshal bet7awel el Argument mn Go Value le JSON Bytes
        // data = el Argument ba3d el Conversion
        // err = ay Error momken ye7sal

        if err != nil {
            // Law el Argument mayenfa3sh yet7awel le JSON

            return fmt.Errorf(
                "encode argument %d: %w",
                i,
                err,
            )
            // Benraga3 Error w n7aded ra2m el Argument
            // %d = Integer
            // %w = Wrap lel Original Error
        }

        req.Args[i] = data
        // Ben7ot el JSON Data fel Request
        // Fel Position bta3 el Argument
    }

    if err := validateRequest(req); err != nil {
        // Bencheck en el Request sa7 abl ma neb3ato
        // Zay Method Name, Number of Arguments, w Size

        return err
        // Law el Request invalid, benraga3 el Error
        // W maneb3atsh 7aga lel Server
    }

    timeout := c.Timeout
    // Ben7ot el Timeout bta3 el Client fe Variable
    // Timeout = aktar Duration el Client hayestana feha

    if timeout <= 0 {
        // Law el Timeout zero aw negative

        timeout = defaultCallTimeout
        // Benesta5dem el Default Timeout
        // 3shan manestannash lel abad
    }

    connection, err := net.DialTimeout(
        "tcp",
        // El Protocol elly hanesta5demo howa TCP

        c.Address,
        // El IP Address wel Port bta3 el Server

        timeout,
        // Maximum Duration lel Connection Attempt
    )
    // DialTimeout bet7awel teftah TCP Connection ma3 el Server
    // connection = el Connection law nag7et
    // err = el Error law failed

    if err != nil {
        // Law el Client ma3rafsh yetesel bel Server

        return fmt.Errorf(
            "connect to server: %w",
            err,
        )
        // Benraga3 Error betwada7 en el Connection failed
    }

    defer connection.Close()
    // Lama el Function tekhlas, el Connection hatet2efel
    // 3shan manseebsh Connections maftoo7a

    if err := connection.SetDeadline(
        time.Now().Add(timeout),
    ); err != nil {
        // SetDeadline bet7aded Deadline lel Read wel Write
        // time.Now() = el Wa2t el 7aly
        // Add(timeout) = Benzawed el Timeout 3ala el Wa2t
        // Law el Deadline et3adda, el Operations momken tefail

        return fmt.Errorf(
            "set call deadline: %w",
            err,
        )
        // Benraga3 Error law ma3rafnash ne7aded el Deadline
    }

    if err := json.NewEncoder(connection).Encode(req); err != nil {
        // NewEncoder bet3mel JSON Encoder 3ala el Connection
        // Encode(req) bet7awel el Request le JSON
        // W beteb3ato lel Server 3abr el TCP Connection

        return fmt.Errorf(
            "send request: %w",
            err,
        )
        // Benraga3 Error law el Request mateb3atesh
    }

    var reply response
    // Ben3araf Variable esmo reply mn Type response
    // Da hayeshil el Response elly gaya mn el Server

    if err := json.NewDecoder(connection).Decode(&reply); err != nil {
        // NewDecoder bet3mel JSON Decoder 3ala el Connection
        // Decode(&reply) bet2ra el JSON elly gaya mn el Server
        // W bet7awelha le Go Response Struct
        // &reply ya3ny Address el Variable 3shan yetmely bel Data

        return fmt.Errorf(
            "read response: %w",
            err,
        )
        // Benraga3 Error law ma3rafnash ne2ra el Response
    }

    if reply.Error != "" {
        // Bencheck law el Server raga3 Error Message
        // != "" ya3ny el Error Message mesh fadya

        return fmt.Errorf(
            "remote error: %s",
            reply.Error,
        )
        // Benraga3 el Error elly geh mn el Server
        // %s = String
    }

    if !reply.OK {
        // Bencheck law el Server ma2alsh en el Request nag7
        // ! ya3ny NOT
        // Law OK = false, el Condition hatb2a True

        return errors.New(
            "remote call failed without an error message",
        )
        // Benraga3 Error 3shan el Call failed
        // Bas el Server mab3atsh Error Message
    }

    if out != nil {
        // Bencheck law el Caller 3ayez ye7faz el Result
        // Law out mesh nil, han7awel el Response lel Type el Matloob

        if err := json.Unmarshal(reply.Value, out); err != nil {
            // Unmarshal bet7awel JSON Data le Go Value
            // reply.Value = el Result elly el Server ba3ato
            // out = el Makan elly han7ot feeh el Result
            // Ghaleban out byeb2a Pointer lel Variable

            return fmt.Errorf(
                "decode result: %w",
                err,
            )
            // Benraga3 Error law el Result mate7awelsh sa7
        }
    }

    return nil
    // Law wasalna hena ya3ny:
    // El Client etesel bel Server
    // El Request etb3at benga7
    // El Response et2aret benga7
    // W el Result et7afaz law out mesh nil
    // Mafish Error
}