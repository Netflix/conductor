import React, {Component} from 'react';
import request from 'superagent';
import { Button, Input, OverlayTrigger, Popover } from 'react-bootstrap';



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

        request
        .post('http://localhost:8080/api/workflow/' + wfname)
        .set('Content-Type', 'application/json')
        .send(data)
        .end(function(err, res){
        console.log(res.text);
        });  

        setTimeout(() => {
            this.setState({ loading: false });
        }, 1000);
      };

      
    render() {

        let inputs = this.state.inputs;
        let loading = this.state.loading;

        const popover = (
            <Popover id="popover-positioned-bottom" title="Workflow executed!">
               Workflow successfully executed.
            </Popover>
         );             
    
        return (
        <div>
            &nbsp;&nbsp;
        {inputs.map((item, idx) => <form>
            
                <Input type="input" label={item} placeholder="Enter the input" onChange={this.handleChange.bind(this, idx)}/>
                    &nbsp;&nbsp;
                </form>)}
                <OverlayTrigger trigger='click' rootClose={true} placement="bottom" overlay={popover}>
                <Button bsStyle="primary" bsSize="large" disabled={loading} onClick={!loading ? this.startWorfklow : null}>{loading ? 'Executing...' : 'Execute workflow'}</Button>
                </OverlayTrigger>
        </div>
        )
    }
}

export default WorkflowMetaInput;