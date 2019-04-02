import React, { PropTypes, Component } from 'react';
import { Link } from 'react-router'
import { connect } from 'react-redux';
import shallowCompare from 'react-addons-shallow-compare';
import { updateSearchAndFetch } from '../../actions/search';


const menuPaths = {
  Workflow: [{
    header: true,
    label: 'Executions',
    href: '/events',
    icon: 'fa-star'
  },{
    label: 'All',
    href: '/workflow',
    icon: 'fa-circle'
  },{
    label: 'Running',
    href: '/workflow?status=RUNNING',
    icon: 'far fa-play-circle'
  },{
    label: 'Failed',
    href: '/workflow?status=FAILED&h=48',
    icon: 'fas fa-exclamation-circle'
  },{
    label: 'Timed Out',
    href: '/workflow?status=TIMED_OUT&h=48',
    icon: 'far fa-clock'
  },{
    label: 'Terminated',
    href: '/workflow?status=TERMINATED&h=48',
    icon: 'fa-ban'
  },{
    label: 'Completed',
    href: '/workflow?status=COMPLETED&h=48',
    icon: 'fa-bullseye'
  },{
    label: 'Scheduled',
    href: '/workflow/scheduled',
    icon: 'far fa-calendar-alt'
  },{
    header: true,
    label: 'Metadata',
    href: '/events',
    icon: 'fa-star'
  },{
    label: 'Workflow Defs',
    href: '/workflow/metadata',
    icon: 'fas fa-code-branch'
  },{
    label: 'Tasks',
    href: '/workflow/metadata/tasks',
    icon: 'fa-tasks'
  },{
    header: true,
    label: 'Workflow Events',
    href: '/events',
    icon: 'fa-star'
  },{
    label: 'Event Handlers',
    href: '/events',
    icon: 'fa-star'
  },{
    header: true,
    label: 'Task Queues',
    href: '/events',
    icon: 'fa-star'
  },{
    label: 'Poll Data',
    href: '/workflow/queue/data',
    icon: 'fas fa-exchange-alt'
  }]
};

class LeftMenu extends Component {
  constructor(props) {
    super(props);

    this.state = {
      loading: false
    };
  }

  shouldComponentUpdate(nextProps, nextState) {
    return shallowCompare(this, nextProps, nextState);
  }

  render() {
    const { loading, minimize, updateSearchAndFetch } = this.props;
    const appName = 'Workflow';

    const width = minimize ? '50px' : '176px';

    const display = minimize ? 'none' : '';
    const menuItems = [];
    let keyVal = 0;

    // eslint-disable-next-line array-callback-return
    menuPaths[appName].map(({icon, header, label, search, href}) => {
      const iconClass = `fa ${icon}`;

      if (header === true) {
        menuItems.push(
          <div className="" key={`key-${(keyVal += 1)}`}>
            <div className="menuHeader">
              <i className="fa fa-angle-down" />&nbsp;{label}
            </div>
          </div>
        );
      } else if (search) {
        menuItems.push(
            <Link to={href} key={`key-${(keyVal += 1)}`} onClick={() => updateSearchAndFetch(search)}>
              <div className="menuItem">
                <i className={iconClass} style={{ width: '20px' }} />
                <span style={{ marginLeft: '10px', display }}>{label}</span>
              </div>
            </Link>
        );
      } else {
        menuItems.push(
          <Link to={href} key={`key-${(keyVal += 1)}`}>
            <div className="menuItem">
              <i className={iconClass} style={{ width: '20px' }} />
              <span style={{ marginLeft: '10px', display }}>{label}</span>
            </div>
          </Link>
        );
      }
    });

    return (
      <div className="left-menu" style={{ width }}>
        <div className="logo textual pull-left">
          <a href="/" title="Frinx Conductor">
            <h4><i className={this.state.loading?"fa fa-bars fa-spin fa-1x":""}></i>{this.state.loading || minimize?'':<img src="/images/FRINX_logo_smaller.png" width="120" alt="Frinx" margin="15px"></img>}</h4>
          </a>
        </div>
        <div className="menuList">{menuItems}</div>
      </div>
    );
  }
}

LeftMenu.propTypes = {
  updateSearchAndFetch: PropTypes.func.isRequired,
  version: PropTypes.string,
  minimize: PropTypes.bool,
  loading: PropTypes.bool.isRequired
};

function mapStateToProps(state) {
  return {loading: state.workflow.fetching}
}

const mapDispatchToProps = {updateSearchAndFetch};

export default connect(mapStateToProps, mapDispatchToProps)(LeftMenu);
