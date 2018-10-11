import React, {Component} from 'react';
import { connect } from 'react-redux';
import { Button, Input, Label, Well, Panel } from 'react-bootstrap';
import WorkflowMetaCron from './WorkflowMetaCron';
import { startWorkflow } from '../../actions/WorkflowActions';
import { Link } from 'react-router';
import { CSSTransitionGroup } from 'react-transition-group'; // ES6


class WorkflowMetaInput extends Component {
    constructor(props) {
        super(props);

        this.startWorfklow = this.startWorfklow.bind(this);

        this.state = {
            jsonData: {},
            log: this.props.res,
            loading: this.props.starting,
            label: "info",
            showCron: false
        }
    }

    componentWillReceiveProps(nextProps){
        this.setState({
            loading: nextProps.starting,
            log: nextProps.res
        });
    }

    handleChange(idx, e) {
        var { labels, values } = this.props.workflowForm;
        var dataObject = {};
        
        if (idx !== -1) {
            values.splice(idx, 1, e.target.value);
        }

        for (let i = 0; i < labels.length; i++) {
            if (values[i] && values[i].startsWith("{")) {
                dataObject[labels[i]] = JSON.parse(values[i]);
            } else if (values[i])
                dataObject[labels[i]] = values[i];
        }
        this.state.jsonData = dataObject;
    };

    startWorfklow(e) {

        e.preventDefault();
        this.handleChange(-1);

        let wfname = this.props.meta.name;
        let data = {
            json: this.state.jsonData,
            ... (this.state.showCron ? {
                cronExp: this.refs.cronform.state.cronExp,
                cronDesc: this.refs.cronform.state.cronDesc
            } : {cronExp: null, cronDesc: null})
        };

        console.log(JSON.stringify(data, null, 2));

        this.props.dispatch(startWorkflow(wfname, data));        
    };

    showCron(){
        this.setState({ showCron: true })
    }

    hideCron(){
        this.setState({ showCron: false})
    }

    render() {
        const { loading, log, showCron } = this.state;
        const values = this.props.workflowForm.values || [];
        const descs = this.props.workflowForm.descs || [];
        const { labels } = this.props.workflowForm;

        function renderDesc(idx) {
            if(descs[idx]){
                return (
                    <Label>{descs[idx]}</Label>
                )
            }
        }

        function renderCronComp() {
            if(showCron){
                return (
                    <WorkflowMetaCron ref="cronform"/>
                )
            }
        }

        function consoleLog() {
            if(log){
                if(log.body && log.statusCode === 200){
                    return (
                        <div>
                            <Well>
                            <span><h4>Workflow ID:</h4><Link to={`/workflow/id/${log.body.text}`}>{log.body.text}</Link><br/></span>
                            <span><h4>Status code: </h4> <Label bsStyle="success">{log.statusCode}</Label><br/></span>
                            <span><h4>Status text: </h4> <Label bsStyle="success">{log.statusText}</Label><br/></span>
                            </Well>
                        </div>
                    );
                }
                if(log.statusCode === 200){
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
                else {
                    return (
                        <div>
                            <Well>
                            <span><h4>Error: </h4> <Label bsStyle="danger">{log.toString()}</Label><br/></span>
                            </Well>
                        </div>
                    )
                } 
            }
        }
  
        return (
            <div className="input-form">
                <Panel header="Execute workflow">
                    <h1>Inputs of <Label bsStyle={log ? (log.error ? "danger":"success"):"info"}>{this.props.name}</Label> workflow</h1>
                        {labels.map((item, idx) => 
                         <form onSubmit={!loading ? this.startWorfklow : null}>
                            &nbsp;&nbsp;
                                <Input type="input" key={values} 
                                                    label={item}
                                                    defaultValue={values[idx]}
                                                    placeholder="Enter the input"
                                                    onChange={this.handleChange.bind(this, idx)}/>
                                 {renderDesc(idx)}
                                 &nbsp;&nbsp;
                         </form>)}

                    <h3>Schedule workflow &nbsp;&nbsp;

                        <Button className="btn btn-default btn-circle" bsSize="xsmall"
                                onClick={showCron ? this.hideCron.bind(this) : this.showCron.bind(this)}>
                            { showCron ?  <i className="fas fa-minus"/> : <i className="fas fa-plus"/> }
                        </Button></h3>

                    <CSSTransitionGroup
                        transitionName="cronanim"
                        transitionEnterTimeout={300}
                        transitionLeaveTimeout={300}>
                            {renderCronComp()}<br/>
                    </CSSTransitionGroup>

                    <Button bsStyle="primary" 
                            bsSize="large" 
                            disabled={loading} 
                            onClick={!loading ? this.startWorfklow : null}>
                            { loading ? <i className="fas fa-spinner fa-spin"/> : <i className="fa fa-play"/>}
                            &nbsp;&nbsp;{loading ? 'Executing...' : 'Execute workflow'}
                    </Button>

                    <h3>Console log</h3>
                        {consoleLog()} 

                </Panel>   
            </div>
        )
    }
}
export default connect(state => state.workflow)(WorkflowMetaInput);