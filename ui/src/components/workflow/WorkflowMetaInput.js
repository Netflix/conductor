import React, {Component} from 'react';
import request from 'superagent';
import { Button, Input, Label, Well } from 'react-bootstrap';



class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.startWorfklow = this.startWorfklow.bind(this);

        this.state = {
        name: props.meta.name,
        inputs: props.meta.inputsArray,
        outputs: [],
        finalString: {},
        loading: false,
        label: "info",
        log: {}
        }
    }

    componentWillReceiveProps(nextProps) {
        this.state.name = nextProps.meta.name;
        this.state.inputs = nextProps.meta.inputsArray;
        this.state.outputs = new Array(nextProps.meta.inputsArray.length)
    };

    handleChange(idx , event){

        var arr = this.state.outputs;
        var names = this.state.inputs;

        arr.splice(idx, 1, event.target.value);

        console.log(arr);
        console.log(this.state.inputs);

        var string = {}
            for (var i = 0; i < names.length; i++) {

                    if(arr[i].startsWith("{")){
                        string[names[i]] = JSON.parse(arr[i])
                    }
                    else
                    string[names[i]] = arr[i]
                }

        console.log(JSON.stringify(string, null, 2));

        this.setState({finalString: JSON.stringify(string, null, 2)})

    }

    startWorfklow(){           
        
        this.setState({ loading: true });

        let wfname = this.state.name;
        let data = this.state.finalString    
        var self = this;

        request
        .post('http://localhost:8080/api/workflow/' + wfname)
        .set('Content-Type', 'application/json')
        .send(data)
        .end(function(err, res){
                if(err){
                    setTimeout(() => {
                        self.setState({ loading: false,
                                        label: "danger",
                                        log: err
                                 });
                           }, 1000);
                }
                else{
                    setTimeout(() => {
                        self.setState({ loading: false,
                                        label: "success",
                                        log: res
                                 });
                           }, 1000);
                }           
        });  


      };

    consoleLog(){    
        if(this.state.label == "success"){
            return (
                <div>
                    <Well>
                    <span><h4>Workflow id: </h4> <Label bsStyle="info">{this.state.log.text}</Label><br/></span>
                    <span><h4>Status code: </h4> <Label bsStyle="success">{this.state.log.statusCode}</Label><br/></span>
                    <span><h4>Status text: </h4> <Label bsStyle="success">{this.state.log.statusText}</Label><br/></span>
                    </Well>
                </div>
            );
        }
        if(this.state.label == "danger") {
            return (
                <div>
                    <Well>
                    <span><h4>Error: </h4> <Label bsStyle="danger">{this.state.log.toString()}</Label><br/></span>
                    </Well>
                </div>
            )
        } 
        
    }

      
    render() {

        let inputs = this.state.inputs;
        let loading = this.state.loading;
  
        return (
        <div>
            &nbsp;&nbsp;
            <h1>Inputs of <Label bsStyle={this.state.label}>{this.state.name}</Label> workflow</h1>
            &nbsp;&nbsp;
        {inputs.map((item, idx) => <form>
                <Input type="input" label={item} placeholder="Enter the input" onChange={this.handleChange.bind(this, idx)}/>
                    &nbsp;&nbsp;
                </form>)}
                <Button bsStyle="primary" bsSize="large" disabled={loading} onClick={!loading ? this.startWorfklow : null}>{loading ? 'Executing...' : 'Execute workflow'}</Button>
                <h3>Console log</h3>
                {this.consoleLog()}    
        </div>
        )
    }
}

export default WorkflowMetaInput;