import React from 'react';
import { connect } from 'react-redux';
import { Panel, Button } from 'react-bootstrap';

class ErrorPage extends React.Component {
  state = {
    alertVisible: false
  };

  componentWillReceiveProps(nextProps) {
    const { error } = nextProps;
    this.setState({ alertVisible: error });
  }

  handleAlertDismiss = () => {
    this.setState({ alertVisible: false });
  };

  render() {
    const { message, stack } = this.props;
    const { alertVisible } = this.state;

    if (alertVisible) {
      return (
        <span className="error">
          <Panel header={message} bsStyle="danger">
            <code>{stack}</code>
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
