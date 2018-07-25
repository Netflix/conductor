import React, {Component} from 'react';
import {JsonEditor} from 'react-json-edit';
import request from 'superagent';
import has from 'lodash/has';



class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.state = {
        name: props.meta.name,
        version: props.meta.version,
        json: props.meta.workflowMeta,
        }


    }

    componentWillReceiveProps(nextProps) {
        this.state.name = nextProps.meta.name;
        this.state.version = nextProps.meta.version;
        this.state.json = nextProps.meta.workflowMeta;
    };

    render() {

        let wfname = this.state.name;
        let jsonInput = JSON.stringify(this.state.json, null, 2);

        var RegExp = /\input([\w.])+\}/igm
        var RegExp2 = /[^\.]+(?=\})/igm

        var matchArray = jsonInput.match(RegExp);

        if(matchArray) {
        var matchString = matchArray.join();
        var sortedArray = matchString.match(RegExp2);
        var inputsArray = _.uniq(sortedArray);
        console.log("Sorted array of inputs:" + inputsArray);
        }
       
      
        function startWorfklow(){                            

            request
            .post('http://localhost:8080/api/workflow/' + wfname)
            .set('Content-Type', 'application/json')
            .send(jsonInput)
            .end(function(err, res){
            console.log(res.text);
            });  
          };

          function printArray(){
              return (
                  
                     inputsArray.map(item => <form>
                        <label>
                            input:
                            <input type="text" value={item} />
                        </label>   
                    </form>)
                 
              )
          };

        
    
        return (
        <div>
        {printArray}
        <button onClick={startWorfklow}>Send Workflow</button>
        </div>
        )
    }
}

export default WorkflowMetaInput;
