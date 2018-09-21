import React, {Component} from 'react';
import request from 'superagent';
import { Button, Input, Label, Well, Panel, Grid, Row, Col, ButtonToolbar } from 'react-bootstrap';

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
            log: {},
            checked: false,
            cronExp: null,
            cronArr: Array(6).fill('*'),
            setButton: true,
            cronDesc: null
        }
    }

    componentWillReceiveProps(nextProps) {
        this.state.name = nextProps.meta.name;
        this.state.labels = nextProps.meta.inputsArray;
        this.state.desc = nextProps.meta.desc;
        this.state.value = nextProps.meta.value;
    };

    handleChange(idx, e) {
        var defValues = this.state.value;
        var inputLabels = this.state.labels;
        var dataObject = {};

        if (idx != -1) {
            defValues.splice(idx, 1, e.target.value);
        }

        for (let i = 0; i < inputLabels.length; i++) {
            if (defValues[i] && defValues[i].startsWith("{")) {
                dataObject[inputLabels[i]] = JSON.parse(defValues[i]);
            } else if (defValues[i])
                dataObject[inputLabels[i]] = defValues[i];
        }

        this.state.jsonData = dataObject;
    };

    handleCron(idx, e) {
        let cronArr = this.state.cronArr;

        if (e.target.value == "") {
            cronArr.splice(idx, 1, "*");
        } else
            cronArr.splice(idx, 1, e.target.value);

        this.state.cronArr = cronArr;
    };

    handleCronDesc(e) {
        this.state.cronDesc = e.target.value;
    };

    submitCron(setButton) {
        let cronArr = this.state.cronArr;
        if (setButton) {
            this.state.cronExp = cronArr.join(" ");
            this.setState({
                setButton: false
            });
        } else {
            this.state.cronExp = null;
            this.setState({
                setButton: true
            });
        }
    };

    startWorfklow(e) {

        e.preventDefault();
        this.handleChange(-1);

        this.setState({
            loading: true
        })

        let self = this;
        let wfname = this.state.name;

        let data = {
            json: this.state.jsonData,
            cronExp: this.state.cronExp,
            cronDesc: this.state.cronDesc
        };

        console.log(JSON.stringify(data, null, 2));

        request
            .post('/api/wfe/workflow/' + wfname)
            .send(data)
            .end(function (err, res) {
                if (err) {
                    setTimeout(() => {
                        self.setState({
                            loading: false,
                            label: "danger",
                            log: err
                        });
                    }, 300);
                } else {
                    setTimeout(() => {
                        self.setState({
                            loading: false,
                            label: "success",
                            log: res
                        });
                    }, 300);
                }
            });
    };

    render() {

        const { loading, value, desc, label, labels, log, name, setButton } = this.state;

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
            <div className="input-form">
                <Panel header="Execute workflow">
                    <h1>Inputs of <Label bsStyle={label}>{name}</Label> workflow</h1>

                    {labels.map((item, idx) => <form onSubmit={!loading ? this.startWorfklow : null}>
                        &nbsp;&nbsp;
                        <Input type="input" key={value} 
                                            label={item} 
                                            defaultValue={value[idx]} 
                                            placeholder="Enter the input" 
                                            onChange={this.handleChange.bind(this, idx)}/>
                        {renderDesc(idx)} 
                        &nbsp;&nbsp;
                        </form>)}
                    <br/>

                    <h3>Schedule workflow</h3>

                    <div className="input-grid">
                        <form>
                            <Grid>
                                <Row className="show-grid">
                                    <Col md={1}>
                                        <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 0)}/>
                                        &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Seconds</label>
                                    </Col>
                                    <Col md={1}>
                                        <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 1)}/>
                                        &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Minutes</label>
                                    </Col>
                                    <Col md={1}>
                                        <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 2)}/>
                                        &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Days</label>
                                    </Col>
                                    <Col md={1}>
                                        <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 3)}/>
                                        &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Weeks</label>
                                    </Col>
                                    <Col md={1}>
                                        <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 4)}/>
                                        &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Months</label>
                                    </Col>
                                    <Col md={1}>
                                        <Input type="input" placeholder="∗" className="input" onChange={this.handleCron.bind(this, 5)}/>
                                        &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Years</label>
                                    </Col>

                                    <ButtonToolbar>
                                        <Button bsStyle={setButton ? "success" : "danger"} 
                                                onClick={() => {this.submitCron(setButton)}}>
                                                <i className="far fa-calendar-alt"/>&nbsp;&nbsp;{setButton ? 'Set' : 'Cancel'}</Button>
                                        <Button bsStyle="info">
                                                <i className="fas fa-question-circle"/>&nbsp;&nbsp;Help</Button>
                                    </ButtonToolbar>

                                </Row>
                            </Grid>
                        </form>
                    </div>

                    <Input type="input" placeholder="Description (optional)" onChange={this.handleCronDesc.bind(this)}/>
            
                    <br/>
                     <Button bsStyle="primary" 
                             bsSize="large" 
                             disabled={loading} 
                             onClick={!loading ? this.startWorfklow : null}>
                             <i className="fa fa-play"/>
                             &nbsp;&nbsp;{loading ? 'Executing...' : 'Execute workflow'}
                    </Button>

                    <h3>Console log</h3>
                    {consoleLog()} 

                </Panel>   
            </div>
        )
    }
}
export default WorkflowMetaInput;