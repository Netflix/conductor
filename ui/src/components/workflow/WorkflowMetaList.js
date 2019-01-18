import React, { Component } from 'react';
import { Link, browserHistory } from 'react-router';
import {Button, Col, Grid, Panel, Row} from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import { connect } from 'react-redux';
import { getWorkflowDefs } from '../../actions/WorkflowActions';
import Typeahead from "react-bootstrap-typeahead";

const WorkflowMetaList = React.createClass({

    getInitialState() {
        return {
            name: '',
            version: '',
            workflows: [],
            workflowsFiltered: [],
            labels: [],
            allLabels: []
        }
    },

    componentWillMount(){
        this.props.dispatch(getWorkflowDefs());
    },

    componentWillReceiveProps(nextProps){
        this.state.workflows = nextProps.workflows;
        this.state.workflowsFiltered = nextProps.workflows;
        this.state.allLabels = this.setAllLabels(nextProps.workflows);
    },

    setAllLabels(wfs) {
        var tags = [];

        for (let key in wfs) {
            let wfsLabels = this.parseDescriptionRegex(wfs[key]);
            if (!wfsLabels.length) {
                continue;
            }
            tags = tags.concat(wfsLabels);
        }
        tags = _.uniq(tags);
        return tags;
    },

    handleSearch() {
        var wfs = this.state.workflows;
        var labels = this.state.labels;

        let filter = [];
        for (let key in wfs) {
            let wfsLabels = this.parseDescriptionRegex(wfs[key]);
            if (!wfsLabels.length) {
                continue;
            }
            if (labels.every(r=> wfsLabels.includes(r))){
                filter.push(wfs[key]);
            }
        }
        if (!labels.length){
            filter = wfs;
        }

        this.setState({
            workflowsFiltered: filter
        })
    },

    //Parse Description to get name of LABELS
    parseDescriptionRegex(workflow){
        var allLabelsPattern = /- [A-Z0-9, ]*/gi;
        var labelsPattern = /[A-Z0-9]+/gi;
        let str = workflow.description;
        let labels = str.match(allLabelsPattern);
        if (!Array.isArray(labels) || !labels.length)
            return [];
        return labels[0].match(labelsPattern);
    },

    handleSubmit(labels) {
        this.state.labels = labels;
        this.handleSearch();
    },

    showAllWorkflows(){
        this.setState({
            workflowsFiltered: this.state.workflows,
            labels: []
        })
    },

    render() {
        var wfs = this.state.workflowsFiltered;

        function jsonMaker(cell, row) {
            return JSON.stringify(cell);
        }

        function taskMaker(cell, row) {
            if(cell == null){
                return '';
            }
            return JSON.stringify(cell.map(task => {return task.name;}));
        }

        function nameMaker(cell, row) {
            return (<Link to={`/workflow/metadata/${row.name}/${row.version}`}>{row.name} / {row.version}</Link>);
        }

        function labelsMaker(cell, row) {
            let str = cell.substring(cell.indexOf("-") + 1);
            if (str === cell)
                str = " ";
            return str;
        }

        return (
            <div className="ui-content">
                <h1>Workflows</h1>
                <div>
                    <Panel header="Filter Workflows (Press Enter to search)">
                    <Grid fluid={true}>
                        <Row className="show-grid">
                            <Col md={1}>
                                <Button onClick={this.showAllWorkflows} className="btn">ALL</Button>
                            </Col>
                            <Col md={8}>
                                <Typeahead ref="workflowTypes" onChange={this.handleSubmit} options={this.state.allLabels} placeholder="Filter by workflow labels" multiple={true} selected={this.state.labels}/>
                                &nbsp;<i className="fa fa-angle-up fa-1x"/>&nbsp;&nbsp;<label className="small nobold">Filter by Workflow Label</label>
                            </Col>
                            <Col md={3}>
                                <Button bsStyle="success" onClick={this.handleSubmit} className="search-label btn"><i className="fa fa-search"/>&nbsp;&nbsp;Search</Button>
                            </Col>
                        </Row>
                    </Grid>
                    <form>

                    </form>
                    </Panel>
                </div>
                <BootstrapTable ref="table" data={wfs} striped={true} hover={true} exportCSV={false} pagination={false} s>
                    <TableHeaderColumn dataField="name" isKey={true} dataAlign="left" dataSort={true} dataFormat={nameMaker}>Name/Version</TableHeaderColumn>
                    <TableHeaderColumn dataField="description" dataFormat={labelsMaker}>Labels</TableHeaderColumn>
                    <TableHeaderColumn dataField="inputParameters" width="500" dataSort={true} dataFormat={jsonMaker}>Input Parameters</TableHeaderColumn>
                    <TableHeaderColumn dataField="tasks" hidden={false} dataFormat={taskMaker}>Tasks</TableHeaderColumn>
                </BootstrapTable>
            </div>
        );
    }
});
export default connect(state => state.workflow)(WorkflowMetaList);
