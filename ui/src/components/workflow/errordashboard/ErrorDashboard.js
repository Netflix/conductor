import React, { Component } from 'react';
import moment from 'moment';
import { Link, browserHistory } from 'react-router';
import { Breadcrumb, BreadcrumbItem, Input, Well, Button, Panel, DropdownButton, Grid, ButtonToolbar, MenuItem, Popover, OverlayTrigger, ButtonGroup, Row, Col, Table } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import Typeahead from 'react-bootstrap-typeahead';
import { connect } from 'react-redux';
import { getQueueData } from '../../../actions/WorkflowActions';

const ErrorDashboard = React.createClass({

  getInitialState() {
    return {
      name: '',
      version: '',
      errorData: []
    }
  },

  componentWillMount(){
    //this.props.dispatch(getErrorData());
  },

  componentWillReceiveProps(nextProps){
    this.state.errorData = nextProps.errorData;
  },

  render() {
    var errorData = this.state.errorData;
    const rangeList = ['All data','This year',
      'Last quarter','This quarter',
      'Last month','This month',
      'Yesterday', 'Today',
      'Last 30 minutes', 'Last 5 minutes'];
    const workflowNames = this.state.workflows?this.state.workflows:[];

    return (
      <div className="ui-content">
        <h1>Workflow Error Dashboard</h1>
         <Panel header="Filter Workflows Errors (Press Enter to search)">
          <Grid fluid={true}>
                     <Row className="show-grid">
                       <Col md={2}>
                         <Typeahead ref="range" onChange={this.rangeChange} options={rangeList} placeholder="Today by default" selected={this.state.range} multiple={true} disabled={this.state.h}/>
                         &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">Filter by date range</label>
                       </Col>
                       <Col md={4}>
                         <Input type="input" placeholder="Search" groupClassName="" ref="search" value={this.state.search} labelClassName="" onKeyPress={this.keyPress} onChange={this.searchChange}/>
                         &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">Free Text Query</label>
                         &nbsp;&nbsp;<input type="checkbox" checked={this.state.fullstr} onChange={this.prefChange} ref="fullstr"/><label className="small nobold">&nbsp;Search for entire string</label>
                         </Col>
                       <Col md={5}>
                         <Typeahead ref="workflowTypes" onChange={this.workflowTypeChange} options={workflowNames} placeholder="Filter by workflow type" multiple={true} selected={this.state.workflowTypes}/>
                         &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">Filter by Workflow Type</label>
                       </Col>
                     </Row>
                      <Row className="show-grid">

                         <Col md={2}>
                                      <Input className="number-input" type="text" ref="h" groupClassName="inline" labelClassName="" label="" value={this.state.h} onChange={this.hourChange}/>
                                      <br/>&nbsp;&nbsp;&nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">Created (in past hours)</label>
                        </Col>
                        <Col md={2}>
                           <input  name="datefrm"  type="date" value={this.state.datefrm} className="form-control"  onChange={ this.dateChangeFrom } />
                            &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">From Date</label>
                         </Col>
                         <Col md={2}>
                            <input  name="dateto"  type="date" value={this.state.dateto} className="form-control"  onChange={ this.dateChangeTo } />
                                &nbsp;<i className="fa fa-angle-up fa-1x"></i>&nbsp;&nbsp;<label className="small nobold">To Date</label>

                           </Col>
                             <Col md={3}>
                                  <Button bsSize="small" bsStyle="success" onClick={this.clearBtnClick}>&nbsp;&nbsp;Clear date range</Button> &nbsp;&nbsp;
                                   <Button bsSize="small" bsStyle="success" onClick={this.exportcsv}>Export Report</Button>&nbsp;&nbsp;
                                   <Button bsSize="medium" bsStyle="success" onClick={this.searchBtnClick} className="fa fa-search search-label">&nbsp;&nbsp;Search</Button>
                              </Col>
                      </Row>
                   </Grid>
         </Panel>
       <Panel header="Null pointer exception">
        <BootstrapTable data={errorData} striped={true} hover={true} exportCSV={false} pagination={false}>
          <TableHeaderColumn dataField="workflow_id" isKey={true} dataAlign="left" dataSort={true}>Workflow ID</TableHeaderColumn>
          <TableHeaderColumn dataField="order_id" dataSort={true} >Order ID</TableHeaderColumn>
          <TableHeaderColumn dataField="job_id" dataSort={true} >Job ID</TableHeaderColumn>
          <TableHeaderColumn dataField="ranking_id" dataSort={true} >Ranking ID</TableHeaderColumn>
          <TableHeaderColumn dataField="failure_time" dataSort={true} >Failure Time</TableHeaderColumn>
          <TableHeaderColumn dataField="complete_message" dataSort={true} >Complete Error Message</TableHeaderColumn>
          </BootstrapTable>
      </Panel>
       <Panel header="Invalid task specified">
              <BootstrapTable data={errorData} striped={true} hover={true} exportCSV={false} pagination={false}>
                <TableHeaderColumn dataField="workflow_id" isKey={true} dataAlign="left" dataSort={true}>Workflow ID</TableHeaderColumn>
                <TableHeaderColumn dataField="order_id" dataSort={true} >Order ID</TableHeaderColumn>
                <TableHeaderColumn dataField="job_id" dataSort={true} >Job ID</TableHeaderColumn>
                <TableHeaderColumn dataField="ranking_id" dataSort={true} >Ranking ID</TableHeaderColumn>
                <TableHeaderColumn dataField="failure_time" dataSort={true} >Failure Time</TableHeaderColumn>
                <TableHeaderColumn dataField="complete_message" dataSort={true} >Complete Error Message</TableHeaderColumn>
                </BootstrapTable>
            </Panel>

             <Panel header="Ping time out">
                    <BootstrapTable data={errorData} striped={true} hover={true} exportCSV={false} pagination={false}>
                      <TableHeaderColumn dataField="workflow_id" isKey={true} dataAlign="left" dataSort={true}>Workflow ID</TableHeaderColumn>
                      <TableHeaderColumn dataField="order_id" dataSort={true} >Order ID</TableHeaderColumn>
                      <TableHeaderColumn dataField="job_id" dataSort={true} >Job ID</TableHeaderColumn>
                      <TableHeaderColumn dataField="ranking_id" dataSort={true} >Ranking ID</TableHeaderColumn>
                      <TableHeaderColumn dataField="failure_time" dataSort={true} >Failure Time</TableHeaderColumn>
                      <TableHeaderColumn dataField="complete_message" dataSort={true} >Complete Error Message</TableHeaderColumn>
                      </BootstrapTable>
                  </Panel>
      </div>
    );
  }
});
export default connect(state => state.workflow)(ErrorDashboard);
