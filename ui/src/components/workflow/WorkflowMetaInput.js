import React, {Component} from 'react';
import {JsonEditor} from 'react-json-edit';
import request from 'superagent';


class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.state = {
        name: props.meta.name,
        version: props.meta.version,
        json: props.meta.workflowMeta,
        jsonTasks: props.meta.workflowMeta.tasks,
        //jsonNode: props.meta.workflowMeta.tasks["0"].inputParameters.node,
        }
    }

    componentWillReceiveProps(nextProps) {
        this.state.name = nextProps.meta.name;
        this.state.version = nextProps.meta.version;
        this.state.json = nextProps.meta.workflowMeta;
        this.state.jsonTasks = nextProps.meta.workflowMeta.tasks;
        //this.state.jsonNode = nextProps.meta.workflowMeta.tasks["0"].inputParameters.node;
    };

    callback = (changes) => {
      this.setState({jsonNode: changes});
    };

    render() {

        let wfname = this.state.name;

        let jsonInput = JSON.stringify(this.state.jsonTasks, null, 2);

        console.log(this.state);
        console.log("json input stringify: " + jsonInput);
               
        function startWorfklow(){                            

            request
            .post('http://localhost:8080/api/workflow/' + wfname)
            .set('Content-Type', 'application/json')
            .send(jsonInput)
            .end(function(err, res){
            console.log(res.text);
            });  
          };

    
        return (
        <div>
        <JsonEditor value={this.state.jsonTasks} propagateChanges={this.callback} tableLike={true}/>
        <button onClick={startWorfklow}>Send Workflow</button>
        </div>
        )
    }
}

export default WorkflowMetaInput;
