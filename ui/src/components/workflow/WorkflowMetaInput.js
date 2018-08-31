import React, {Component} from 'react';
import request from 'superagent';
import { Button, Input, Label, Well } from 'react-bootstrap';
import { Link } from 'react-router';

class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.startWorfklow = this.startWorfklow.bind(this);

        this.state = {
        name: props.meta.name,
        labels: props.meta.inputsArray,
        desc: props.meta.desc,
        value: props.meta.value,
        jsonData: {},
        loading: false,
        label: "info",
        log: {}
        }
    }

    componentWillReceiveProps(nextProps) {
        this.state.name = nextProps.meta.name;
        this.state.labels = nextProps.meta.inputsArray;
        this.state.desc = nextProps.meta.desc;
        this.state.value = nextProps.meta.value;
    };

    handleChange(idx, event) {
        var defValues = this.state.value;
        var inputLabels = this.state.labels;
        var dataObject = {};

        if(idx != -1){
            defValues.splice(idx, 1, event.target.value);
        }

        console.log(defValues);
        console.log(this.state.labels);

        for (let i = 0; i < inputLabels.length; i++) {
            if (defValues[i] && defValues[i].startsWith("{")) {
                dataObject[inputLabels[i]] = JSON.parse(defValues[i]);
            } else if (defValues[i])
                dataObject[inputLabels[i]] = defValues[i];
        }

        console.log(JSON.stringify(dataObject, null, 2));

        this.state.jsonData = dataObject;
    };

    startWorfklow(e){     
        
        e.preventDefault();
        this.handleChange(-1);
        
        this.setState({loading: true})

        let wfname = this.state.name;
        let data = this.state.jsonData;    
        let self = this;

        request
        .post('/api/wfe/workflow/' + wfname)
        .send(data)
        .end(function(err, res){
                if(err){
                    setTimeout(() => {
                        self.setState({ loading: false,
                                        label: "danger",
                                        log: err
                                 });
                           }, 300);
                }
                else{
                    setTimeout(() => {
                        self.setState({ loading: false,
                                        label: "success",
                                        log: res
                                 });
                           }, 300);
                }           
        });
      };
      
    render() {

        let inputs = this.state.labels; 
        let loading = this.state.loading;
        let value = this.state.value;
        let desc = this.state.desc;
        let label = this.state.label;
        let log = this.state.log;

        function renderDesc(idx) {
            if(desc[idx]){
                return (
                    <Label>{desc[idx]}</Label> 
                )
            }
        }

        function consoleLog() {
            if(label == "success"){
                return (
                    <div>
                        <Well>
                        <span><h4>{log.text}</h4><br/></span>
                        <span><h4>Status code: </h4> <Label bsStyle="success">{log.statusCode}</Label><br/></span>
                        <span><h4>Status text: </h4> <Label bsStyle="success">{log.statusText}</Label><br/></span>
                        </Well>
                    </div>
                );
            }
            if(label == "danger") {
                return (
                    <div>
                        <Well>
                        <span><h4>Error: </h4> <Label bsStyle="danger">{log.toString()}</Label><br/></span>
                        </Well>
                    </div>
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
                {consoleLog()}    
        </div>
        )
    }
}
export default WorkflowMetaInput;