import React from 'react';
import { connect } from 'react-redux';
import { Panel, Button } from 'react-bootstrap';

class ErrorPage extends React.Component {
  state = {
    alertVisible: false,
    status: '',
    details: ''
  };

  componentWillReceiveProps(nextProps) {
    const { message: status, stack: details, error } = nextProps;

    this.setState({
      alertVisible: nextProps.error,
      status,
      details
    });
  }

  handleAlertDismiss = () => {
    this.setState({ alertVisible: false });
  };

  render() {
    if (this.state.alertVisible) {
      return (
        <span className="error">
          <Panel header={this.state.status} bsStyle="danger">
            <code>{this.state.details}</code>
          </Panel>
          <Button bsStyle="danger" onClick={this.handleAlertDismiss}>
            Close
          </Button>
          &nbsp;&nbsp;If you think this is not expected, file a bug with workflow admins.
        </span>
      );
    }
    return <span />;
  }
}

export default connect(state => ({
  message: state.workflow.exception.message,
  stack: state.workflow.exception.stack,
  error: state.workflow.error
}))(ErrorPage);
