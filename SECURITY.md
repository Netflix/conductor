## Conductor Server Authentication & Authorization - Roles

All the roles mentioned below are generic and can be overridden as per the need, from conductor-server application.properties --> security.oauth2.resource.mapping

Depending on the OAuth2.0 provider, the UserInfoUrl needs to be configued, in conductor-server application.properties --> security.oauth2.resource.userInfoUri

Finally the path from which to extract roles, from the response json of UserInfoUrl needs to be configured, in conductor-server application.properties --> security.oauth2.resource.userInfoUriParserPath

##### Category of APIs available at Conductor level.
	
		☐  Event Services - For Event Handling APIs
		
		☐  Workflow Management - For workflow executing, rerun, terminate, pause etc. functionalities.
		
		☐  Metadata Management - Workflow or task creation / updation / deletion etc. functionalities.
		
		☐  Health Check - Ignore for now
		
		☐  Admin - Ignore for now
		
		☐  Workflow Bulk Management - For workflow bulk executing, rerun, terminate, pause etc. functionalities.
		
		☐  Task Management - For task executing, rerun, terminate, pause etc. functionalities.
		

##### Roles that are mapped to APIs
		
		role_conductor_super_manager
		role_conductor_super_viewer
		role_conductor_core_manager
		role_conductor_core_viewer
		role_conductor_execution_manager
		role_conductor_execution_viewer
		role_conductor_event_manager
		role_conductor_event_view
		role_conductor_metadata_manager
		role_conductor_metadata_viewer
		role_conductor_metadata_workflow_manager
		role_conductor_metadata_workflow_viewer
		role_conductor_metadata_taskdefs_manager
		role_conductor_metadata_taskdefs_viewer
		role_conductor_workflow_manager
		role_conductor_workflow_viewer
		role_conductor_task_manager
		role_conductor_task_viewer

Technically a Worker would need role_conductor_task_manager, role_conductor_event_manager and role_conductor_execution_manager roles.

##### Technical mapping to roles.
		
		☐  All Manager roles will be able to Create/Update/Delete the mentioned API implemented functionalities.
		
		☐  All Viewer roles will be able to View existing API implemented functionalities.
		
		☐  A default user for each role is created while the flyway migration happens and the username is same as the role (example - 'role_conductor_super_manager') and the password is 'password'
	  		
			1) role_conductor_super_manager - POST / PUT / DELETE
				
			    Event Services
			    Workflow Management
			    Metadata Management
			    Health Check
			    Admin
			    Workflow Bulk Management
			    Task Management
			
			2) role_conductor_super_viewer - GET
				
			    Event Services
			    Workflow Management
			    Metadata Management
			    Health Check
			    Admin
			    Workflow Bulk Management
			    Task Management
			
			3) role_conductor_core_manager - POST / PUT / DELETE
			    Event Services
			    Workflow Management
			    Metadata Management
			    Workflow Bulk Management
			    Task Management
			
			4) role_conductor_core_viewer - GET
				
			    Event Services
			    Workflow Management
			    Metadata Management
			    Workflow Bulk Management
			    Task Management
			
			5) role_conductor_execution_manager - POST / PUT / DELETE
				
			    Event Services
			    Workflow Management
			    Task Management
			
			6) role_conductor_execution_viewer - GET
				
			    Event Services
			    Workflow Management
			    Task Management
			
			7) role_conductor_event_manager - POST / PUT / DELETE
				
			  	Event Services
			  
			8) role_conductor_event_viewer - GET
			  	
			  	Event Services
			
			9) role_conductor_metadata_manager - POST / PUT / DELETE
				
			    Metadata Management
			
			10) role_conductor_metadata_viewer - GET
			
			    Metadata Management
			
			11) role_conductor_workflow_manager - POST / PUT / DELETE
				
			  	Workflow Management
			  
			12) role_conductor_workflow_viewer - GET
			  	
			  	Workflow Management
			
			13) role_conductor_task_manager - POST / PUT / DELETE
				
			  	Task Management
			  
			14) role_conductor_task_viewer - GET
				
			  	Task Management
