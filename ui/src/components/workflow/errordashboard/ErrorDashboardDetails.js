import React, { Component } from 'react';
import moment from 'moment';
import { Link, browserHistory } from 'react-router';
import { Breadcrumb, BreadcrumbItem, Input, Well, Button, Panel, DropdownButton, Grid, ButtonToolbar, MenuItem, Popover, OverlayTrigger, ButtonGroup, Row, Col, Table } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import Typeahead from 'react-bootstrap-typeahead';
import { connect } from 'react-redux';
import { getQueueData } from '../../../actions/WorkflowActions';

const ErrorDashboardDetails = React.createClass({

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
    return (
      <div className="ui-content">
        <h1>Transcode job failed</h1>
         <BootstrapTable data={errorData} striped={true} hover={true} exportCSV={false} pagination={false}>
                <TableHeaderColumn dataField="workflow_id" isKey={true} dataAlign="left" dataSort={true}>Workflow ID</TableHeaderColumn>
                <TableHeaderColumn dataField="order_id" dataSort={true} >Order ID</TableHeaderColumn>
                <TableHeaderColumn dataField="job_id" dataSort={true} >Job ID</TableHeaderColumn>
                <TableHeaderColumn dataField="ranking_id" dataSort={true} >Ranking ID</TableHeaderColumn>
                <TableHeaderColumn dataField="failure_time" dataSort={true} >Failure Time</TableHeaderColumn>
                <TableHeaderColumn dataField="complete_message" dataSort={true} >Complete Error Message</TableHeaderColumn>
                </BootstrapTable>
      </div>
    );
  }
});
export default connect(state => state.workflow)(ErrorDashboardDetails);
