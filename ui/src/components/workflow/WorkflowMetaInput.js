import React, {Component} from 'react';
import request from 'superagent';
import { Button, Input, Label, Well, Tooltip } from 'react-bootstrap';



class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.startWorfklow = this.startWorfklow.bind(this);

        this.state = {
        name: props.meta.name,
        inputs: props.meta.inputsArray,
        desc: props.meta.desc,
        value: props.meta.value,
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
        this.state.desc = nextProps.meta.desc;
        this.state.value = nextProps.meta.value;
        this.state.outputs = new Array(nextProps.meta.inputsArray.length)
    };

    handleChange(idx, event) {
        var arr = this.state.value;
        var names = this.state.inputs;
        var string = {};

        if(idx != -1){
            arr.splice(idx, 1, event.target.value);
        }

        console.log(arr);
        console.log(this.state.inputs);

        for (let i = 0; i < names.length; i++) {
            if (arr[i] && arr[i].startsWith("{")) {
                string[names[i]] = JSON.parse(arr[i]);
            } else if (arr[i])
                string[names[i]] = arr[i];
        }

        console.log(JSON.stringify(string, null, 2));

        this.state.finalString = JSON.stringify(string, null, 2);

    };

    startWorfklow(e){     
        
        e.preventDefault();
        this.handleChange(-1);
        
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
        let value = this.state.value;
        let desc = this.state.desc;

        function renderDesc(idx) {
            if(desc[idx]){
                return (
                    <Label>{desc[idx]}</Label> 
                )
            }
        }
  
        return (
        <div>
            &nbsp;&nbsp;
            <h1>Inputs of <Label bsStyle={this.state.label}>{this.state.name}</Label> workflow</h1>
            &nbsp;&nbsp;
        {inputs.map((item, idx) => <form onSubmit={!loading ? this.startWorfklow : null}>
                &nbsp;&nbsp;
                <Input type="input" key={this.state.value} label={item} defaultValue={value[idx]} placeholder="Enter the input" onChange={this.handleChange.bind(this, idx)}/>
                {renderDesc(idx)} 
                &nbsp;&nbsp;
                </form>)}
                <Button bsStyle="primary" bsSize="large" disabled={loading} onClick={!loading ? this.startWorfklow : null}><i className="fa fa-play"/>&nbsp;&nbsp;{loading ? 'Executing...' : 'Execute workflow'}</Button>
                <h3>Console log</h3>
                {this.consoleLog()}    
        </div>
        )
    }
}

export default WorkflowMetaInput;