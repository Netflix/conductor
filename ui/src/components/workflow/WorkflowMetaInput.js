import React, {Component} from 'react';
import {JsonEditor} from 'react-json-edit';
import request from 'superagent';
import has from 'lodash/has';



class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.state = {
        name: props.meta.name,
        inputs: props.meta.inputsArray,
        outputs: [],
        finalString: {},
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

    render() {

        let wfname = this.state.name;
        let inputs = this.state.inputs;
        let data = this.state.finalString;   
        
        console.log(data);

        function startWorfklow(){                            

            request
            .post('http://localhost:8080/api/workflow/' + wfname)
            .set('Content-Type', 'application/json')
            .send(data)
            .end(function(err, res){
            console.log(res.text);
            });  
          };

                
    
        return (
        <div>
        {inputs.map((item, idx) => <form>
                        <label>{item} + {idx}</label> 
                        <input name={item} type="text" onChange={this.handleChange.bind(this, idx)} />  
                    </form>)}
        <button onClick={startWorfklow}>Send Workflow</button>
        </div>
        )
    }
}

export default WorkflowMetaInput;