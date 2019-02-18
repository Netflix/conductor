import React, {Component} from 'react';
import {OverlayTrigger, Button, Popover, Panel, Table, Input, Well, Label} from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import {connect} from 'react-redux';
import {getWorkflowDetails} from '../../../actions/WorkflowActions';
import WorkflowAction from './WorkflowAction';
import WorkflowMetaDia from '../WorkflowMetaDia';
import moment from 'moment';
import http from '../../../core/HttpClient';
import Clipboard from 'clipboard';
import map from "lodash/fp/map";
import Tab from "../../common/Tab";
import TabContainer from "../../common/TabContainer";
import { startWorkflow } from '../../../actions/WorkflowActions';
import {Link} from "react-router";
import UnescapeButton from '../../common/UnescapeButton'

new Clipboard('.btn');

function formatDate(dt) {
    if (dt == null || dt === '') {
        return '';
    }
    return moment(dt).format('MM/DD/YYYY, HH:mm:ss:SSS');
}

function execTime(end, start) {
    if (end == null || end === 0) {
        return "";
    }

    let total = end - start;

    return total / 1000;
}

function workerLink(type, cell) {
    if (cell == null) {
        return "";
    }
    let href = sys['env'][type] || '#';
    if (href !== '#') {
        href = href.replace('%s', cell);
    } else {
        href = sys['env']['WORKER_LINK'];
        href = href || '#';
        href = href.replace('%s', cell);
    }
    return <a target="_new" href={href}>{cell}</a>;
}

function popoverLink(cell, row) {
    return (<OverlayTrigger trigger="click" rootClose placement="left" overlay={
        <Popover id="task-details-wfd" title="Task Details" style={{width: '800px'}}>
            <Panel header={<span><span>Task Input</span> <i title="copy to clipboard" className="btn fa fa-clipboard"
                                                            data-clipboard-target="#input"/><UnescapeButton target='input' /></span>}>

                <span className="small"><pre id="input">{JSON.stringify(row.inputData, null, 2)}</pre></span>
            </Panel>
            <Panel header={<span><span>Task Output</span> <i title="copy to clipboard" className="btn fa fa-clipboard"
                                                             data-clipboard-target="#output"/><UnescapeButton target='output' /></span>}>
                <span className="small"><pre id="output">{JSON.stringify(row.outputData, null, 2)}</pre></span>
            </Panel>
            <Panel header="Task Failure Reason (if any)">
                <span className="small">{JSON.stringify(row.reasonForIncompletion, null, 2)}</span>
            </Panel>
        </Popover>

    }><Button bsStyle="default" bsSize="xsmall">Input/Output</Button></OverlayTrigger>);
}

function tableRow(task) {
    return <tr key={task.seq}>
        <td>{task.seq}</td>
        <td>{task.taskType}</td>
        <td>{task.referenceTaskName}</td>
        <td>{formatDate(task.scheduledTime)}</td>
        <td>{formatDate(task.startTime)} <br/> {formatDate(task.endTime)}</td>
        <td>{task.status}</td>
        <td>{formatDate(task.updateTime)}</td>
        <td>{task.callbackAfterSeconds == null ? 0 : task.callbackAfterSeconds}</td>
        <td>{task.pollCount}</td>
        <td>{popoverLink(null, task)}<br/>{workerLink(task.taskType, task.workerId)}</td>
    </tr>
}

function tableBody(tasks) {
    return <tbody>{map(tableRow)(tasks)}</tbody>
}

function getFailureReason(wf) {
    return wf.reasonForIncompletion;
}

function showFailure(wf) {
    if (wf.status === 'FAILED' || wf.status === 'TERMINATED' || wf.status === 'TIMED_OUT') {
        return '';
    }
    return 'none';
}

class WorkflowDetails extends Component {
    constructor(props) {
        super(props);

        this.state = {
            loading: this.props.starting,
            log: this.props.res,
            jsonData: null,
            show: false,
            workflowForm: {
                labels: [],
                values: []
            }
        };

        http.get('/api/sys/').then((data) => {
            window.sys = data.sys;
        });
    }

    componentWillReceiveProps(nextProps) {
        this.getObject(nextProps.data);
        if (this.props.hash !== nextProps.hash) {
            this.props.dispatch(getWorkflowDetails(nextProps.params.workflowId));
            this.setState({show: false});
        }

        this.setState({
            loading: nextProps.starting,
            log: nextProps.res,
        });
    }

    shouldComponentUpdate(nextProps) {
        if (nextProps.refetch) {
            this.props.dispatch(getWorkflowDetails(nextProps.params.workflowId));
            return false;
        }
        return true;
    }

    startWorkflow1(e) {

        e.preventDefault();
        let wfname = this.props.meta.name;
        let data = {
            json: this.state.jsonData
        };

        console.log(JSON.stringify(data, null, 2));

        this.props.dispatch(startWorkflow(wfname, data));

            this.setState({ show: true });

    }

    getObject(wf) {
        let inputObject;

        if(!wf.input){
            inputObject = {};
        } else {
            inputObject = wf.input;
        }

        let labels = [];
        let values = [];
        let index = 0;

        if (wf) {

            for (let key in inputObject) {
                labels[index] = key;
                if (typeof inputObject[key] === 'object') {
                    values[index] = JSON.stringify(inputObject[key]);
                } else
                    values[index] = inputObject[key];

                index++;
            }

            if (!this.state.jsonData || _.isEmpty(this.state.jsonData)) {
                this.state.jsonData = inputObject;
            }
            this.state.workflowForm.labels = labels;
            this.state.workflowForm.values = values;
        }

    }

    handleChange(idx, e) {

        let dataObject = {};
        let { labels, values } = this.state.workflowForm;

            values.splice(idx, 1, e.target.value);

        for (let i = 0; i < labels.length; i++) {
            if (values[i] && values[i].startsWith("{")) {
                dataObject[labels[i]] = JSON.parse(values[i]);
            } else if (values[i])
                dataObject[labels[i]] = values[i];
        }
        this.state.jsonData = dataObject;
    }


    render() {

        const { labels, values } = this.state.workflowForm;
        const { log, show } = this.state;

        let wf = this.props.data;
        if (wf == null) {
            wf = {};
        }
        if (wf.tasks == null) {
            wf.tasks = [];
        }
        let tasks = wf['tasks'];
        tasks = tasks.sort(function (a, b) {
            return a.seq - b.seq;
        });

        function consoleLog() {
            if(show) {
                if (log) {
                    if (log.body && log.statusCode === 200) {
                        return (
                            <div>
                                <Well>
                                <span><h4>Workflow ID:</h4><Link
                                    to={`/workflow/id/${log.body.text}`}>{log.body.text}</Link><br/></span>
                                    <span><h4>Status code: </h4> <Label
                                        bsStyle="success">{log.statusCode}</Label><br/></span>
                                    <span><h4>Status text: </h4> <Label
                                        bsStyle="success">{log.statusText}</Label><br/></span>
                                </Well>
                            </div>
                        );
                    }
                    if (log.statusCode === 200) {
                        return (
                            <div>
                                <Well>
                                    <span><h4>{log.text}</h4><br/></span>
                                    <span><h4>Status code: </h4> <Label
                                        bsStyle="success">{log.statusCode}</Label><br/></span>
                                    <span><h4>Status text: </h4> <Label
                                        bsStyle="success">{log.statusText}</Label><br/></span>
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
        }

        return (
            <div className="ui-content">
                <h4>
                    {wf.workflowType}/{wf.version}
                    <span
                        className={(wf.status === 'FAILED' || wf.status === 'TERMINATED' || wf.status === 'TIMED_OUT') ? "red" : "green"}>
          {wf.status}
        </span>
                    <span>
          <WorkflowAction workflowStatus={wf.status} workflowId={wf.workflowId}/>
        </span>
                </h4>
                <br/><br/>
                <Table responsive={true} striped={false} hover={false} condensed={false} bordered={true}>
                    <thead>
                    <tr>
                        <th>Workflow ID</th>
                        <th>Owner App</th>
                        <th>Total Time (sec)</th>
                        <th>Start/End Time</th>
                        <th>Correlation ID</th>
                    </tr>
                    <tr>
                        <td>{wf.workflowId}</td>
                        <td>{wf.ownerApp}</td>
                        <td>{execTime(wf.endTime, wf.startTime)}</td>
                        <td>{formatDate(wf.startTime)} - {formatDate(wf.endTime)}</td>
                        <td>{wf.correlationId}</td>
                    </tr>
                    <tr style={{display: showFailure(wf)}}>
                        <td style={{color: '#ff0000'}} colSpan={5}>{getFailureReason(wf)}</td>
                    </tr>
                    </thead>
                </Table>

                <TabContainer>
                    <Tab eventKey={1} title="Execution Flow">
                        <WorkflowMetaDia meta={this.props.meta} wfe={wf} subworkflows={this.props.subworkflows}/>
                    </Tab>
                    <Tab eventKey={2} title="Task Details">
                        <Table responsive={true} striped={true} hover={true} condensed={false} bordered={true}>
                            <thead>
                            <tr>
                                <th>#</th>
                                <th>Task Type</th>
                                <th>Task Ref. Name</th>
                                <th>Scheduled Time</th>
                                <th>Start/End Time</th>
                                <th>Status</th>
                                <th>Last Polled/Updated</th>
                                <th>Callback After</th>
                                <th>Poll Count</th>
                                <th>Details</th>
                            </tr>
                            </thead>
                            {tableBody(tasks)}
                        </Table>
                        <br/>
                    </Tab>
                    <Tab eventKey={3} title="Input/Output">
                        <div>
                            <strong>Workflow Input <i title="copy to clipboard" className="btn fa fa-clipboard"
                                                      data-clipboard-target="#wfinput"/></strong>
                                                      <UnescapeButton target='wfinput' />
                            <pre style={{height: '200px'}} id="wfinput">{this.state.unescapeInput ? unescapeJs(JSON.stringify(wf.input, null, 3)) : JSON.stringify(wf.input, null, 3)}</pre>
                            <strong>Workflow Output <i title="copy to clipboard" className="btn fa fa-clipboard"
                                                       data-clipboard-target="#wfoutput"/></strong>
                                                       <UnescapeButton target='wfoutput' />
                            <pre style={{height: '200px'}}
                                 id="wfoutput">{JSON.stringify(wf.output == null ? {} : wf.output, null, 3)}</pre>
                            {wf.status === 'FAILED' ? <div><strong>Workflow Faiure Reason (if any)</strong>
                                <pre>{wf.reasonForIncompletion ? JSON.stringify(wf.reasonForIncompletion, null, 3) : ''}</pre>
                            </div> : ''}
                        </div>
                    </Tab>
                    <Tab eventKey={4} title="JSON">
                        <i title="copy to clipboard" className="btn fa fa-clipboard"
                           data-clipboard-target="#fulljson"/>
                           <UnescapeButton target='fulljson' />
                        <pre style={{height: '80%'}} id="fulljson">{JSON.stringify(wf, null, 3)}</pre>
                    </Tab>
                    <Tab eventKey={5} title="Edit input">
                        &nbsp;&nbsp;

                        <div className="input-form">
                            <Panel header="Rerun workflow">
                                <h1>Inputs of <Label
                                    bsStyle={log ? (log.error ? "danger" : "success") : "info"}>{this.props.meta ? this.props.meta.name : ""}</Label> workflow
                                </h1>
                                {labels.map((item, idx) =>
                                    <form onSubmit={!this.state.loading ? this.startWorkflow1 : null}>
                                        &nbsp;&nbsp;
                                        <Input type="input" key={values}
                                               label={item}
                                               defaultValue={values[idx]}
                                               placeholder="Enter the input"
                                               onChange={this.handleChange.bind(this, idx)}/>
                                        &nbsp;&nbsp;
                                    </form>)}

                                <Button bsStyle="primary" bsSize="large" disabled={this.state.loading}
                                        onClick={!this.state.loading ? this.startWorkflow1 : null }>
                                        <i className="fas fa-redo"/>
                                        &nbsp;&nbsp;Rerun workflow
                                </Button>

                                <h3>Console log</h3>
                                {consoleLog()}

                            </Panel>
                        </div>

                    </Tab>
                </TabContainer>
            </div>
        );
    }
}

export default connect(state => state.workflow)(WorkflowDetails);
