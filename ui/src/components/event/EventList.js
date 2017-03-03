import React, { Component } from 'react';
import { Link, browserHistory } from 'react-router';
import { Breadcrumb, BreadcrumbItem, Input, Well, Button, Panel, DropdownButton, MenuItem, Popover, OverlayTrigger, ButtonGroup, Table } from 'react-bootstrap';
import {BootstrapTable, TableHeaderColumn} from 'react-bootstrap-table';
import { connect } from 'react-redux';
import { getEventHandlers } from '../../actions/WorkflowActions';

const Events = React.createClass({

  getInitialState() {
    return {
      events: []
    }
  },
  componentWillMount(){
    this.props.dispatch(getEventHandlers());
  },
  componentWillReceiveProps(nextProps) {
    this.state.events = nextProps.events || [];
  },

  render() {
    var wfs = this.state.events;

    function jsonMaker(cell, row){
      return JSON.stringify(cell);
    };

    function activeLink(cell, row){
      return cell?'Yes':'No';
    };
    function helpName() {
      return (<OverlayTrigger trigger="click" rootClose placement="bottom" overlay={
        <Popover title='Event Handler Name' style={{ width: '300px'}}>
          <div className="info">
            Unique name identifying the event handler.
          </div>
        </Popover>
      }><a><i className="fa fa-question-circle"></i></a></OverlayTrigger>);
    }
    function helpQueue() {
      //<i className="fa fa-question-circle"></i>
      return (<OverlayTrigger trigger="click" rootClose placement="bottom" overlay={
        <Popover title='Event / Queue' style={{ width: '500px'}}>
          <div className="info">
            <p>Name of the Queue which the handler listens to.  The supported queue systems are <b>SQS</b>  and <b>Conductor</b>.</p>
            <p>The name is prefixed by the source (sqs, conductor).  e.g. sqs:sqs_queue_name</p>
            <p>For SQS this is the name of the queue and NOT the URI of the queue.</p>
            <p>For Conductor the name is same as 'sink' name provided for Event tasks.</p>
          </div>
        </Popover>
      }><a><i className="fa fa-question-circle"></i></a></OverlayTrigger>);
    }
    function helpCond() {
      //<i className="fa fa-question-circle"></i>
      return (<OverlayTrigger trigger="click" rootClose placement="bottom" overlay={
        <Popover title='Condition' style={{ width: '500px'}}>
          <div className="info">
            <p>An expression that can be evaluated with the payload in the queue.</p>
            <p>The Actions are executed ONLY when the expression evaluation returns True</p>
            <p>The expression follows Javascript for syntax</p>
            <p>An empty / null expression is evaluated to True</p>
          </div>
        </Popover>
      }><a><i className="fa fa-question-circle"></i></a></OverlayTrigger>);
    }
    function helpActions() {
      //<i className="fa fa-question-circle"></i>
      return (<OverlayTrigger trigger="click" rootClose placement="bottom" overlay={
        <Popover title='Actions' style={{ width: '500px'}}>
          <div className="info small">
            <p>Set of actions that are taken when a message arrives with payload that matches the condition.</p>
            <p>Supported Actions are: start_workflow, complete_task and fail_task</p>
            <p>For the detailed documentation on the syntax and parameters for each of these actions, visit the doumentation link</p>
          </div>
        </Popover>
      }><a><i className="fa fa-question-circle"></i></a></OverlayTrigger>);
    }
    function nameMaker(cell, row){
      return (<OverlayTrigger trigger="click" rootClose placement="right" overlay={
        <Popover title={row.name} style={{ width: '500px'}}><div className="left">
          <pre>{JSON.stringify(row, null, 2)}</pre>
        </div></Popover>
      }><a>{cell}</a></OverlayTrigger>);
    };
    function getActions(eh) {
      let trs = [];
      eh.actions.forEach(action => {
        let row = <div><b>{action.action}</b><pre>{JSON.stringify(action[action.action], null, 2)}</pre></div>
        trs.push(row);
      });
      return <div>{trs}</div>;
    }
    function tableBody(events) {
      let trs = [];
      events.forEach(eh => {
        let row = <tr>
                    <td>{nameMaker(eh.name, eh)}</td>
                    <td>{eh.event}</td>
                    <td>{eh.condition}</td>
                    <td>{getActions(eh)}</td>
                    <td>{eh.active?'Yes':'No'}</td>
                  </tr>;
        let actionRows = <tr><td colspan='4'>{getActions(eh)}</td></tr>
        trs.push(row);
      });
      return <tbody>{trs}</tbody>
    }

    return (
      <div className="ui-content">
        <h1>Event Handlers</h1>
        <Table responsive={true} striped={true} hover={true} condensed={false} bordered={true}>
        <thead>
          <tr>
            <th>Name {helpName()}</th>
            <th>Event / Queue {helpQueue()}</th>
            <th>Condition {helpCond()}</th>
            <th>Actions {helpActions()}</th>
            <th>Active?</th>
          </tr>
        </thead>
          {tableBody(wfs)}
        </Table>
      </div>
    );
  }
});
export default connect(state => state.workflow)(Events);
