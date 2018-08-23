import React from 'react';
import uuid from 'uuid';
import { connect } from 'react-redux';
import { Input, Popover, OverlayTrigger } from 'react-bootstrap';
import { BootstrapTable, TableHeaderColumn } from 'react-bootstrap-table';
import { getTaskDefs } from '../../../actions/WorkflowActions';

class TaskMetaList extends React.Component {
  state = {
    taskDefs: []
  };

  componentWillMount() {
    this.props.dispatch(getTaskDefs());
  }

  componentWillReceiveProps({ taskDefs }) {
    this.setState({ taskDefs });
  }

  render() {
    const { taskDefs } = this.state;

    const retries = (_, row) => {
      return row.retryLogic === 'FIXED' ? `${row.retryLogic} (${row.retryDelaySeconds} seconds)` : '';
    };

    function editor(cell, row) {
      return (
        <OverlayTrigger
          trigger="click"
          rootClose
          placement="right"
          overlay={
            <Popover id={`popover-id-${uuid.v4()}`} title={row.name} style={{ width: '500px' }}>
              <div className="left">
                <form>
                  <Input type="text" value={row.retryCount} addonBefore="Retry Count" addonAfter="Times" readOnly />
                  <br />
                  <Input type="select" value={row.retryLogic} addonBefore="Retry Logic" readOnly>
                    <option value="FIXED">FIXED</option>
                    <option value="EXPONENTIAL_BACKOFF">EXPONENTIAL_BACKOFF</option>
                  </Input>
                  <br />
                  <Input
                    type="text"
                    defaultValue={row.retryDelaySeconds}
                    addonBefore="Retry Delay"
                    addonAfter="Seconds"
                    readOnly
                  />
                  <br />
                  <Input type="select" defaultValue={row.timeoutPolicy} addonBefore="Time Out Action" readOnly>
                    <option value="RETRY_TASK">RETRY TASK</option>
                    <option value="TIME_OUT_WF">TIME_OUT_WF</option>
                  </Input>
                  <br />
                  <Input
                    type="text"
                    defaultValue={row.timeoutSeconds}
                    addonBefore="Time Out"
                    addonAfter="Seconds"
                    readOnly
                  />
                  <br />
                  <Input
                    type="text"
                    defaultValue={row.responseTimeoutSeconds}
                    addonBefore="Response Time Out"
                    addonAfter="timeoutSeconds"
                    readOnly
                  />
                  <br />
                  <Input
                    type="text"
                    ref="concurrentExecLimit"
                    value={row.concurrentExecLimit}
                    addonBefore="Concurrent Exec Limit"
                  />
                  <br />
                  <Input
                    type="text"
                    ref="rateLimitPerFrequency"
                    value={row.rateLimitPerFrequency}
                    addonBefore="Rate Limit Amount"
                  />
                  <br />
                  <Input
                    type="text"
                    ref="rateLimitFrequencyInSeconds"
                    value={row.rateLimitFrequencyInSeconds}
                    addonBefore="Rate Limit Frequency"
                    addonAfter="Seconds"
                  />
                  <br />
                  <Input type="textarea" label="Task Description" defaultValue={row.description} readOnly />
                  <br />
                </form>
              </div>
            </Popover>
          }
        >
          <a>{cell}</a>
        </OverlayTrigger>
      );
    }

    return (
      <div className="ui-content">
        <h1>Task Definitions</h1>
        <BootstrapTable data={taskDefs} striped hover search exportCSV={false} pagination={false}>
          <TableHeaderColumn dataField="name" isKey dataAlign="left" dataSort dataFormat={editor}>
            Name/Version
          </TableHeaderColumn>
          <TableHeaderColumn dataField="ownerApp" dataSort>
            Owner App
          </TableHeaderColumn>
          <TableHeaderColumn dataField="timeoutPolicy" dataSort>
            Timeout Policy
          </TableHeaderColumn>
          <TableHeaderColumn dataField="timeoutSeconds" dataSort>
            Timeout Seconds
          </TableHeaderColumn>
          <TableHeaderColumn dataField="responseTimeoutSeconds" dataSort>
            Response Timeout Seconds
          </TableHeaderColumn>
          <TableHeaderColumn dataField="retryCount" dataSort>
            Retry Count
          </TableHeaderColumn>
          <TableHeaderColumn dataField="concurrentExecLimit" dataSort>
            Concurrent Exec Limit
          </TableHeaderColumn>
          <TableHeaderColumn dataField="rateLimitPerFrequency" dataSort>
            Rate Limit Amount
          </TableHeaderColumn>
          <TableHeaderColumn dataField="rateLimitFrequencyInSeconds" dataSort>
            Rate Limit Frequency Seconds
          </TableHeaderColumn>
          <TableHeaderColumn dataField="retryLogic" dataSort dataFormat={retries}>
            Retry Logic
          </TableHeaderColumn>
        </BootstrapTable>
      </div>
    );
  }
}

export default connect(state => state.workflow)(TaskMetaList);
