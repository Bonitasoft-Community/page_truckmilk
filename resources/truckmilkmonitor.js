'use strict';
/**
 * 
 */

(function() {

	var appCommand = angular.module('truckmilk', [ 'googlechart', 'ui.bootstrap','ngMaterial', 'angularFileUpload' ]);

	// --------------------------------------------------------------------------
	//
	// Controler TruckMilk
	//
	// --------------------------------------------------------------------------

	// Ping the server
	appCommand.controller('TruckMilkControler', function($http, $scope, $sce, $timeout, $upload, $filter) {

		this.showhistory = function(show) {
			this.isshowhistory = show;
		}
		this.navbaractiv='Reports';
		
		this.getNavClass = function( tabtodisplay )
		{
			if (this.navbaractiv === tabtodisplay)
				return 'ng-isolate-scope active';
			return 'ng-isolate-scope';
		}

		this.getNavStyle = function( tabtodisplay )
		{
			if (this.navbaractiv === tabtodisplay)
				return 'border: 1px solid #c2c2c2;border-bottom-color: transparent;';
			return 'background-color:#cbcbcb';
		}
		
		
		this.inprogress = false;

		// return the class used for the label
		this.getClassExecution =  function( status ) {
			if (! status)
				return "";
			
			if (status.toUpperCase() === 'SUCCESS' || status.toUpperCase() ==='SUCCESSPARTIAL')
				return "label label-success btn-xs";
			if (status.toUpperCase() === 'ERROR' || status.toUpperCase()=== 'BADCONFIGURATION')
				return "label label-danger btn-xs";
			return "label label-default";
		}
		
		
		this.listplugtour =[];
		
		this.listplugin = [];
		this.newtourname = '';
		this.refreshDate= new Date();
	
		
		
		this.delayListScope= [ {"value": "YEAR", "label":"Year"},
			{"value": "MONTH", "label":"Month"},
			{"value": "WEEK", "label":"Week"},
			{"value": "DAY", "label":"Day"},
			{"value": "HOUR", "label":"Hour"},
			{"value": "MN", "label":"Minutes"} ];
		
		// a new Jobs is receive : refresh all what we need in the
		// interface
		this.refreshListJobs = function() {
			// console.log("refreshListJobs:Start");
			for ( var i in this.listplugtour) {
				var milkJobindex = this.listplugtour[i];
				this.prepareJobParameter(milkJobindex);
			}
			
			// calculate a uniq time stamp, to avoid the infinit loop when
			// calculate the downloadParameterFile
			this.refreshDate = new Date();
			
			this.calculateParameterUpload();
		}

		// No new jobs, just refresh the status. So, refresh all savecexectution 
		// interface
		this.refreshListReportExecution = function() {
			// collect all trackedsaveexecution
			// console.log("refreshListReportExecution:Build list reportExecution");
			this.listreportsexecution=[];
			for ( var i in this.listplugtour) {
				var milkjob = this.listplugtour[i];
				// console.log("  JobIndex "+angular.toJson(jobindex));
				if (milkjob.savedExecution) {
					for (var j in milkjob.savedExecution) {
						
						// console.log('savedExecution[j] '+ angular.toJson(jobindex.savedExecution[ j ]));
						var reportExecution = angular.fromJson( angular.toJson(milkjob.savedExecution[ j ]));

						reportExecution.pluginname	= milkjob.pluginname;
						reportExecution.name		= milkjob.name;
						reportExecution.milkjob		= milkjob;
						this.listreportsexecution.push( reportExecution );
					}
				}
			}
			// newest in first
			this.listreportsexecution= $filter('orderBy')(this.listreportsexecution, 'execDate',true);
			// console.log('refreshListReportExecution.order='+angular.toJson(this.listreportsexecution));
			
		}

		
		// -----------------------------------------------------------------------------------------
		// getListPlugin
		// -----------------------------------------------------------------------------------------

		this.plugin = { 'showProcesses':true, 'showCases' : true, 'showTasks':true, 'showMonitor':true, 'showOthers':true};

		this.displayPlugIn = function (plugIn ) {
			if (plugIn.category === "PROCESSES" && this.plugin.showProcesses)
				return true;
			if (plugIn.category === "CASES" && this.plugin.showCases)
				return true;
			if (plugIn.category === "TASKS" && this.plugin.showTasks)
				return true;
			if (plugIn.category === "MONITOR" && this.plugin.showMonitor)
				return true;
			
			if (plugIn.category === "OTHER" && this.plugin.showOthers)
				return true;
			return false;
		}
		
		this.getListPlugIn = function() {
			// console.log("getListPlugIn:Start (r/o)");
			return this.listplugin;
		}
		this.getListPlugTour = function() {
			// consoleCall HTTPgTour:Start (r/o)");
			return this.listplugtour;
		}

		this.orderByFilter = {
				'milkTourField' : 'name',
				'milkTourReverse': false,
				'reportExecution': 'today'
		};
		this.setOrderMilkJob = function( name ) {
			this.orderByFilter.milkTourField= name; 
			this.orderByFilter.milkTourReverse = ! this.orderByFilter.milkTourReverse;
			// console.log("change listTourOrder ["+this.orderByFilter.milkTourField+"] reverse is now "+this.orderByFilter.milkTourReverse);
		}
		this.getListPlugTourOrder = function() {
			// console.log("listTourOrder ["+this.orderByFilter.milkTourField+"] reverse="+this.orderByFilter.milkTourReverse);
			var sortedItems = $filter('orderBy')(this.listplugtour, this.orderByFilter.milkTourField, this.orderByFilter.milkTourReverse);
			return sortedItems;
		}
		
		// Filter
		this.getListReportExecutionFiltered = function() {
			if (! this.listreportsexecution)
				return [];
			var tagdayfilter= this.todaytagday;
			if (this.orderByFilter.reportExecution == 'all')
				return this.listreportsexecution;
			if (this.orderByFilter.reportExecution =='today')
				tagdayfilter=this.todaytagday;
			if (this.orderByFilter.reportExecution =='yesterday')
				tagdayfilter= this.todaytagday-1;
			var report= $filter('filter')(this.listreportsexecution, { 'tagDay':tagdayfilter} );
			return report;
			
		}
		
		this.loadinit = function() {
			var self = this;
			
			self.inprogress = true;
			self.showUploadSuccess=false;

			// console.log("loadinit inprogress<=true. Call now");
			self.listevents='';
			
			// console.log("loadinit Call HTTP");
			$http.get('?page=custompage_truckmilk&action=startup&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}

				// console.log("loadinit.receiveData (scheduler)");
				
				self.listplugtour 	= jsonResult.listplugtour;
				self.listplugin 	= jsonResult.listplugin;
				self.scheduler 		= jsonResult.scheduler;
				self.listevents 	= jsonResult.listevents;
				self.deploimentsuc  = jsonResult.deploimentsuc;
				self.deploimenterr  = jsonResult.deploimenterr;
				self.todaytagday  	= jsonResult.todaytagday;

				// prepare default value
				self.refreshListJobs();
				self.refreshListReportExecution();

				self.inprogress = false;
				

			}).error(function(jsonResult, statusHttp, headers, config) {
				console.log("loadinit.error HTTP statusHttp="+statusHttp);

				// connection is lost ?
				if (statusHttp==401) {
					console.log("Redirected to the login page !");
					window.location.reload();
				}

				self.inprogress = false;

				self.listplugtour = [];
				self.listplugin = [];

			});

		}

		this.autorefresh=false;
		this.showResetJobs=false;
		this.completeStatus=false;
		
		
		// completeStatus : refresh the complete list + getStatus on scheduler,
		// else refresh only some attributes
		this.refresh = function( completeStatus ) {
			var self = this;
			self.deploimentsuc=''; // no need to keep it for ever
			// console.log("refresh.start listplugtour="+angular.toJson( self.listplugtour )); 
			// console.log("refresh.start");
			if (self.inprogress)
			{
				console.log("Refresh in progress, skip this one");
				return;
			}
			
			// close all popup
			for (var i in this.listplugtour)
			{
				var milkJob = this.listplugtour[ i ];
				this.hideall( milkJob);
			}
			
			var verb="refresh";
			if (completeStatus)
				verb="getstatus";
			
			self.completeStatus = completeStatus;
			self.listevents='';
			self.inprogress = true;
			self.showUploadSuccess=false;

			// console.log("action["+verb+"] inprogress<=true. Call now 2.2");
			$http.get('?page=custompage_truckmilk&action='+verb+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				self.inprogress = false;
				console.log("success.then HTTP inprogress<=true, status="+statusHttp);
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}

					
				self.deploiment 	= jsonResult.deploiment;
				self.todaytagday  	= jsonResult.todaytagday;

				// console.log("Result completeStatus ? "+self.completeStatus);
				self.refreshPlugTourFromServer(self.completeStatus, self, jsonResult );


				self.listplugin 	= jsonResult.listplugin;
				self.listevents 	= jsonResult.listevents;				

			}).error( function(jsonResult, statusHttp, headers, config){
				console.log("refresh.error HTTP statusHttp="+statusHttp);
				// connection is lost ?
				if (statusHttp==401) {
					console.log("Redirected to the login page !");
					window.location.reload();
				}

				self.inprogress = false;
				self.listplugtour = [];
				self.listplugin = [];
			});

		}
		// 
		this.refreshPlugTourFromServer = function( completeStatus, self, jsonResult ) {
			console.log("refreshPlugTourFromServer - complete="+completeStatus);
		
			if (completeStatus)
			{
				console.log("refreshPlugTour True, update all  (scheduler)"+angular.toJson( self.scheduler)); 
	
				self.listplugtour 	= jsonResult.listplugtour;
				self.refreshListJobs();
				self.refreshListReportExecution();
		
				self.scheduler 		= jsonResult.scheduler;
				// console.log("Scheduler="+angular.toJson( self.scheduler));
	
			}
			else
			{
				console.log("refreshPlugTour completeStatus False"); 
				for (var i in jsonResult.listplugtour)
				{
					var serverPlugTour = jsonResult.listplugtour[ i ];
					var foundIt = false;
					for ( var j in self.listplugtour) {
						var localPlugTour = self.listplugtour[ j ];
						// console.log("Compare
						// server["+serverPlugTour.name+"]("+serverPlugTour.id+")
						// with
						// ["+localPlugTour.name+"]("+localPlugTour.id+")");
						if (serverPlugTour.id == localPlugTour.id)
						{
							// console.log("refreshPlugTour: Found it ["+localPlugTour.id+"]-["+localPlugTour.name+"]");
							foundIt= true;
							// localPlugTour.name = serverPlugTour.name;
							// cron
							localPlugTour.trackExecution					= serverPlugTour.trackExecution;
							localPlugTour.lastexecutionlistevents			= serverPlugTour.lastexecutionlistevents;
							localPlugTour.enable							= serverPlugTour.enable;
							localPlugTour.hostsrestriction					= serverPlugTour.hostsrestriction;
							localPlugTour.savedExecution					= serverPlugTour.savedExecution;
							localPlugTour.askForStop						= serverPlugTour.askForStop;
							localPlugTour.listevents						= serverPlugTour.listevents;
							localPlugTour.parameters						= serverPlugTour.parameters;
							localPlugTour.measurement						= serverPlugTour.measurement;
							localPlugTour.name								= serverPlugTour.name;
							// console.log("Values does not change:"+angular.toJson(localPlugTour.parametersvalue));
						}
	
					}
					if (! foundIt)
					{
						// add it in the list
						// console.log("Found it !");
						// self.listplugtour.push(serverPlugTour );
					}
				}	
				self.refreshListJobs();

				self.refreshListReportExecution();

			}
		}
		
		
		this.addJob = function(plugin, nameJob) {
			// console.log("addJob: add["+nameJob+"]");
			var message="";
			
			if (! nameJob || nameJob === '') {
				message += "Job Name is mandatory;"
			}
			if (! plugin || plugin ==='') {
				message += "Select a plug in;"
			}
			if (message !== '') {
				alert(message);
				return;				
			}

			var param = {
				'plugin' : plugin,
				'name' : nameJob
			};
			this.operationJob('addJob', param, null, true);
		}

		
		this.setLimitationItems = function(milkjob) {
			if (! milkjob.stopafterNbItemsIN) { 
				milkjob.stopafterNbItems=0
			}			
		}
		
		this.setLimitationHour = function(milkjob) {
			if (! milkjob.stopafterNbMinutesIN) { 
				milkjob.stopafterNbMinutes=0
			}			
		}
		
		this.removeJob = function(milkJob) {
			var result = confirm("Do you want to remove this job?");
			if (result) {
				// console.log("removeJob:Start");

				var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};
				this.operationJob('removeJob', param, null, false);
			}
		}

		this.abortJob = function( milkJob) {
			// console.log("abortTour:Start");
			var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};
			this.operationJob('abortJob', param, milkJob, false);
		}

		// Activate / Deactivate
		this.activateJob = function(milkJob) {
			// console.log("startJob:Start");
			var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};
			this.operationJob('activateJob', param, milkJob, false);
		}
		
		this.deactivateJob = function(milkJob) {
			// console.log("stopJob:beginning");
			var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};

			this.operationJob('deactivateJob', param, milkJob, true);		
		}

		var milkJobSelf;
		
		this.updateJob= function(milkJob) {
			// console.log("updateJob START")
			var paramStart = milkJob;
			var self=this;
			// update maybe very heavy : truncate it
			self.listeventsexecution="";
			milkJobSelf = milkJob;
			
			milkJob.listevents='';
			// first action will be a reset
			// prepare the string
			var  milkJobcopy =angular.copy( milkJob );
			milkJobcopy.parametersdef = null; // parameters are in
												// milkJobcopy.parametersvalue
			milkJobcopy.parameters = null;
			milkJobcopy.lastexecutionlistevents=null;
			milkJobcopy.savedExecution = null;
			
			var json = angular.toJson( milkJobcopy, false);
			
			self.sendPost('updateJob', json );
		}
		
		
		
		this.immediateJob = function(milkJob) {
			// console.log("immediateJob:Start");
			var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};

			this.operationJob('immediateExecution', param, milkJob, true);		
		}
		
		this.abortJob = function(milkJob) {
			// console.log("abortJob:begin");
			var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};

			this.operationJob('abortJob', param, milkJob, true);		
		}
		
		this.resetJob = function(milkJob) {
			// console.log("resetJob:begin");
			var param = {
					'name' :  milkJob.name,
					'id'   :  milkJob.id
				};

			this.operationJob('resetJob', param, milkJob, true);		
		}
		
		// execute an operation on Job
		this.operationJob = function(action, param, milkJob, refreshParam) {
			// console.log("operationJob:Start");
			
			var self = this;
			self.inprogress = true;
			console.log("operationJob START ["+action+"] inprogress<=true");

			self.action = action;
			self.addlistevents= "";
			self.listevents = "";
			self.currentplugtour = milkJob;
			self.refreshParam = refreshParam;
			var json = encodeURIComponent(angular.toJson(param, true));
			self.listevents='';
			// console.log("operationJob Call HTTP");

			$http.get('?page=custompage_truckmilk&action=' + action + '&paramjson=' + json+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				self.inprogress = false;
				// console.log("operationJob.receiveData HTTP inprogress<=false");
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				
				if (jsonResult.listplugtour != undefined)
				{
					// todo keep the one open
					self.listplugtour = jsonResult.listplugtour;
					self.refreshListJobs();
					self.refreshListReportExecution();

					self.refreshParam=true; // force it to true
				}
				if (jsonResult.enable != undefined && self.currentplugtour != undefined)
				{	
					// console.log("enable=", jsonResult.enable);
					self.currentplugtour.enable = jsonResult.enable;
				}

				if (self.currentplugtour != null) {
					self.currentplugtour.listevents = jsonResult.listevents;
				}
				else if (self.action==='addJob') {
					self.addlistevents = jsonResult.listevents;
				}
				else {
					self.listevents = jsonResult.listevents;
				}
				if (self.refreshParam) {
					
					console.log("operationtour.Refresh them");
					// prepare defaut value
					self.refreshListJobs();
					self.refreshListReportExecution();
					
				}
		}).error(function(jsonResult, statusHttp, headers, config) {
			console.log("operatinoJob.error HTTP statusHttp="+statusHttp);
			// connection is lost ?
			if (statusHttp==401) {
				console.log("Redirected to the login page !");
				window.location.reload();
			}

			self.inprogress = false;		
			self.listplugtour = [];
			self.listplugin = [];

		});

		}

		// prepareJobParameter
		// we copy the different parameters from parameters[SourceRESTAPI] to
		// parametersvalue [ANGULAR MAPPED]
		this.prepareJobParameter = function(milkJob) {
			
			// console.log("prepareJobParameter.start: " + milkJob.name);
			milkJob.newname=milkJob.name;
			milkJob.parametersvalue = {};
			
			for ( var key in milkJob.parameters) {
				milkJob.parametersvalue[key] = JSON.parse( JSON.stringify( milkJob.parameters[key]) );
				// console.log("Parameter[" + key + "] value[" + angular.toJson(milkJob.parameters[key]) + "] ="+angular.toJson(milkJob.parametersvalue,true ));
			}
			// prepare all test button
			for (var key in milkJob.parametersdef)
			{
				var parameterName=milkJob.parametersdef[ key ].name;
				var parameterType = milkJob.parametersdef[ key ].type;
				if (parameterType==="BUTTONARGS")
				{

					// console.log("buttonARGS detected key=["+key+"]
					// name=["+buttonName+"]
					// args="+angular.toJson(milkJob.parametersdef[ key
					// ].args));
					// set the default value
					var listArgs=milkJob.parametersdef[ key ].args;
					var mapArgsValue={};
					milkJob.parametersvalue[ parameterName ]=mapArgsValue;
					for (var i in listArgs)
					{
						var argname=listArgs[ i ].name;
						mapArgsValue[ argname ] = listArgs[ i ].value;
					}
				}
				if (parameterType==="ARRAYPROCESSNAME" || parameterType==='ARRAY' || parameterType==='ARRAYMAP')
				{
					// give a empty list
					if (! milkJob.parametersvalue[ parameterName ]) {
						milkJob.parametersvalue[ parameterName ] = [];
					}
				}
			}
			if (milkJob.stopafterNbItems == 0)
				milkJob.stopafterNbItemsIN = false;
			else
				milkJob.stopafterNbItemsIN = true;
			if (milkJob.stopafterNbMinutes == 0)
				milkJob.stopafterNbMinutesIN = false;
			else
				milkJob.stopafterNbMinutesIN = true;
			// console.log("milkJobPreration END " + milkJob.name+" milkJob.parametersvalue="+angular.toJson( milkJob.parametersvalue,true ));

		}
		
		this.isVisibleParameter = function ( milkJob, parameterdef ) {
			$scope.milkJob=milkJob;
			$scope.milkParameter=parameterdef;
			var isVisible = true;
			if (parameterdef.visibleCondition) {
				isVisible = $scope.$eval(parameterdef.visibleCondition);
				// console.log("isVisibleParameter milkJob["+milkJob.name+"] parameter["+parameterdef.name+"] result="+isVisible);
			}
			return isVisible;
		}
		// -----------------------------------------------------------------------------------------
		// manupulate array in parameters
		this.isAnArray = function( value ) {			
		    return Array.isArray( value );
	     }
		this.addInArray = function( valueArray, valueToAdd )
		{
			// console.log("addInArray valueArray="+angular.toJson( valueArray
			// ));
			
			valueArray.push(  valueToAdd );
			// console.log("add valueArray="+angular.toJson( valueArray ));
		}
		this.removeInArray = function( valueArray, value)
		{
			// console.log("removeInArray value "+value);
			var index = valueArray.indexOf(value);
			if (index > -1) {
				valueArray.splice(index, 1);
			}
			// console.log("remove value ["+value+"] index ["+index+"]
			// valueArray="+angular.toJson( valueArray ));
		}
		
		// -----------------------------------------------------------------------------------------
		// Button Test
		// -----------------------------------------------------------------------------------------
		// just return an array with the same size as the nbargs
		this.getArgs = function( parameterdef )
		{			
			 return new Array(parameterdef.nbargs);  
		}
		
		// click on a test button
		var currentButtonExecution=null;
		this.testbutton = function(parameterdef, milkJob)
		{
			// console.log("testbutton:Start");
			var self=this;
			var  milkJobcopy =angular.copy( milkJob );
			milkJobcopy.parametersdef = null; // parameters are in
												// milkJobcopy.parametersvalue
			milkJobcopy.parameters = null;
			milkJobcopy.savedExecution = null;
			milkJobcopy.buttonName= parameterdef.name;
			milkJobcopy.args = milkJob.parametersvalue[ parameterdef.name ];
			
			// console.log("testbutton with "+angular.toJson( milkJobcopy ));
			parameterdef.listeventsexecution="";
			self.currentButtonExecution = parameterdef;	
			parameterdef.listeventsexecution="";
			self.sendPost('testButton', angular.toJson( milkJobcopy ) );
		}
				
		// -----------------------------------------------------------------------------------------
		// parameters tool
		// -----------------------------------------------------------------------------------------
		this.query = function(queryName, textSearch, parameterDef) {
			// console.log("query:Start");
			var selfParameterDef=parameterDef;
			var self=this;
			self.inprogress=true;
			// console.log("operationTour.query ["+queryName+"] on ["+textSearch+"] inprogress<=true");

			var param={ 'userfilter' :  textSearch, 'filterProcess':parameterDef.filterProcess};
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the
			// cache, then create a different URL
			var d = new Date();
			
			// console.log("query Call HTTP")
			return $http.get( '?page=custompage_truckmilk&action='+queryName+'&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult, statusHttp, headers, config ) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress=false;
				// console.log("Query.receiveData HTTP inProgress<=false result="+angular.toJson(jsonResult.data, false));
				selfParameterDef.list 			=  jsonResult.data.listProcess;
				selfParameterDef.nbProcess		=  jsonResult.data.nbProcess;
				return selfParameterDef.list;
			}, function ( jsonResult ) {
				console.log("QueryUser HTTP THEN");
				self.inprogress=false;

			});

		  };
		  
		// -----------------------------------------------------------------------------------------
		// Show information
		// -----------------------------------------------------------------------------------------
		this.hideall = function(milkJob) {
			milkJob.show = {
				'schedule' : false,
				'threaddump' : false,
				'parameters' : false,
				'report' : false,
				'analysis' : false,
				'hostrestriction':false,
				'dashboard':false
			};
		}
		this.showthreaddump = function( milkJob ) {
			// console.log("showthreaddump milkJob="+angular.toJson( milkJob));
			var self=this;
			self.inprogress=true;
			
			this.hideall(milkJob);
			milkJob.show.threaddump=true;
			
			var param={"id": milkJob.id };
			var milkJobSelf = milkJob;
			milkJob.threadDump="";
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the
			// cache, then create a different URL
			var d = new Date();
			
			// console.log("query Call HTTP")
			return $http.get( '?page=custompage_truckmilk&action=threadDumpJob&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult, statusHttp, headers, config ) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress=false;
				// console.log("showthreaddump result="+angular.toJson(jsonResult.data, false));
				milkJobSelf.threadDump 			=  jsonResult.data.threadDump;
				return;
			}, function ( jsonResult ) {
				console.log("showthreaddump HTTP THEN");
				self.inprogress=false;
			});

		}
		this.showSchedule = function(milkJob) {
			// console.log("showSchedule:Start");
			this.hideall(milkJob);
			milkJob.show.schedule = true;
		}
		this.showHostRestriction = function(milkJob) {
			// console.log("showHost Restriction:Start");
			this.hideall(milkJob);
			milkJob.show.hostrestriction = true;
		}

		this.showparameters = function(milkJob) {
			// console.log("showparameters:Start");

			this.hideall(milkJob);
			milkJob.show.parameters = true;
			
			var self=this;
			self.inprogress=true;
			
			var param={"id": milkJob.id, "c":milkJob.c,"p":milkJob.p };
			var milkJobSelf = milkJob;
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the
			// cache, then create a different URL
			var d = new Date();
			
			// console.log("query Call HTTP")
			return $http.get( '?page=custompage_truckmilk&action=getParameters&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult, statusHttp, headers, config ) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress=false;
				// console.log("getParameters result="+angular.toJson(jsonResult.data, false));
				milkJobSelf.parameters 			=  jsonResult.data.milkjob.parameters;
				milkJobSelf.parametersdef 		=  jsonResult.data.milkjob.parametersdef;
				self.prepareJobParameter(milkJobSelf);
				return;
			}, function ( jsonResult ) {
				console.log("getSavedExecution HTTP THEN");
				self.inprogress=false;
			});
			
		}
		this.showreport = function(milkJob, c, p) {
			// console.log("showreport:Start");
			this.hideall(milkJob);
			milkJob.show.report = true;
			milkJob.c=c;
			milkJob.p=p;
			
			// console.log("showReport milkJob="+angular.toJson( milkJob));
			var self=this;
			self.inprogress=true;
			
			var param={"id": milkJob.id, "c":milkJob.c,"p":milkJob.p };
			var milkJobSelf = milkJob;
			milkJob.threadDump="";
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the
			// cache, then create a different URL
			var d = new Date();
			
			// console.log("query Call HTTP")
			return $http.get( '?page=custompage_truckmilk&action=getSavedExecution&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult, statusHttp, headers, config ) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress=false;
				// console.log("getSavedExecution result="+angular.toJson(jsonResult.data, false));
				milkJobSelf.savedExecution 			=  jsonResult.data.milkjob.savedExecution;
				milkJobSelf.savedExecutionMoreData	=  jsonResult.data.milkjob.savedExecutionMoreData;
				milkJobSelf.trackExecution			=  jsonResult.data.milkjob.trackExecution;
				return;
			}, function ( jsonResult ) {
				console.log("getSavedExecution HTTP THEN");
				self.inprogress=false;
			});
			
			
		}
		
		this.showreportplus = function( milkJob ) {
			milkJob.c = milkJob.c+5;
			this.showreport( milkJob,milkJob.c, 0 );			
		}
		/**
		 * 
		 */
		this.showreportdetail= function( milkJob, savedExec, fromTabReport ) {
			// console.log("showReportDetail milkJobId="+ milkJob.id);
			var self=this;
			self.inprogress=true;
			self.fromTabReport= fromTabReport;
			var param={"id": milkJob.id, "savedExecutionDateSt":savedExec.execDatest };
			var milkJobSelf = milkJob;
			var savedExecSelf = savedExec;
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the
			// cache, then create a different URL
			var d = new Date();
			
			// console.log("query Call HTTP")
			return $http.get( '?page=custompage_truckmilk&action=getSavedExecutionDetail&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult, statusHttp, headers, config ) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress=false;
				// console.log("getSavedExecution result="+angular.toJson(jsonResult.data, false));
				savedExecSelf.reportinhtml 			=  jsonResult.data.milkjob.savedExecution[0].reportinhtml;
				savedExecSelf.listevents 			=  jsonResult.data.milkjob.savedExecution[0].listevents;
				if (self.fromTabReport) {
					self.showdetail=true;
				}
				return;
			}, function ( jsonResult ) {
				console.log("showreportdetail HTTP THEN");
				self.inprogress=false;
			});
			
		}
		
		this.showdashboard = function(milkJob) {
			// console.log("showreport:Start");
			this.hideall(milkJob);
			milkJob.show.dashboard = true;
			// console.log("showReport milkJob="+angular.toJson( milkJob));
			var self=this;
			self.inprogress=true;
			
			var param={"id": milkJob.id, "c":milkJob.c,"p":milkJob.p };
			var milkJobSelf = milkJob;
			milkJob.threadDump="";
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the
			// cache, then create a different URL
			var d = new Date();
			
			// console.log("query Call HTTP")
			return $http.get( '?page=custompage_truckmilk&action=getMeasurement&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult, statusHttp, headers, config ) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress=false;
				// console.log("getmeasurement result="+angular.toJson(jsonResult.data, false));
				milkJobSelf.measurement 			=  jsonResult.data.milkjob.measurement;
				return;
			}, function ( jsonResult ) {
				console.log("getSavedExecution HTTP THEN");
				self.inprogress=false;
			});
		}
		this.showanalysis = function(milkJob) {
			// console.log("showreport:Start");
			this.hideall(milkJob);
			milkJob.show.analysis = true;
		}
		this.hasanalysis = function( milkJob) {
			if (milkJob && milkJob.analysisdef)
				return milkJob.analysisdef.length > 0 ;
			return false;
		}
		this.getJobStyle= function(milkJob) {
			// console.log("getTourStyle:Start (r/o) milkJob="+milkJob.id+"
			// imediateExecution="+milkJob.imediateExecution+"
			// inExecution="+milkJob.trackExecution.inExecution+"
			// enable="+milkJob.enable+" askForStop="+milkJob.askForStop);
			
			/*
			 * no more color if (milkJob.imediateExecution) return
			 * "background-color: #fafabd80"; if (milkJob.inExecution &&
			 * milkJob.askForStop) return "background-color: #ffa5004f"; if
			 * (milkJob.inExecution) return "background-color: #f6f696";
			 * 
			 * if (milkJob.lastexecutionstatus == "ERROR") return
			 * "background-color: #e7c3c3"; if (milkJob.lastexecutionstatus ==
			 * "WARNING") return "background-color: #fcf8e3"; if
			 * (milkJob.enable) return "background-color: #ebfae5";
			 */
		}
		this.getNowStyle= function( milkJob )
		{
			// console.log("getNowStyle:Start (r/o) milkJob="+milkJob.id);
			if (milkJob.trackExecution.imediateExecution)
				return "btn btn-success btn-xs"
			return "btn btn-info btn-xs";
		}		
		this.getNowTitle=function( milkJob )
		{
			// console.log("getNowTitle:Start (r/o) milkJob="+milkJob.id);
			if (milkJob.trackExecution.imediateExecution)
				return "An execution will start as soon as possible, at the next round"
			return "Click to have an immediat execution (this does not change the schedule, or the activate state)";
		}
		
		this.getAbortStyle= function( milkJob )
		{
			return "btn btn-warning btn-xs"
		}		
		this.getAbortTitle=function( milkJob )
		{
			return "Click to ask the job to stop as soon as possible, at the end of it's current transaction";
		}
		this.getResetStyle= function( milkJob )
		{
			return "btn btn-danger btn-xs"
		}		
		this.getResetTitle=function( milkJob )
		{
			return "Click to kill immediately the job";
		}
		
		// -----------------------------------------------------------------------------------------
		// Report parameters
		// -----------------------------------------------------------------------------------------
		this.downloadParameterFile = function(milkJob, parameterdef)
		{
			// console.log("downloadParameterFile:Start (r/o)
			// ["+parameterdef.name+"]");
			var param={"plugintour": milkJob.id, "parametername":parameterdef.name};			
			var json = encodeURIComponent(angular.toJson(param, true));
			// do not calculate a new date now:we will have a recursive call in
			// Angular
			return "?page=custompage_truckmilk&action=downloadParamFile&paramjson=" + json+'&t='+this.refreshDate.getTime();
		}
		
		
		// calculate listParameterUpload
		this.listParameterUpload=[];
		this.parameterToUpload=null;
		
		this.calculateParameterUpload = function() {
			// console.log("calculateParameterUpload - start");
			this.listParameterUpload = [];
			for ( var i in this.listplugtour) {
				var milkJob = this.listplugtour[i];
				// console.log("calculateParameterUpload - look
				// "+plugtour.name);
				for ( var key in milkJob.parametersdef) {
					var parameter = milkJob.parametersdef[ key ];
					// console.log("calculateParameterUpload - look
					// "+milkJob.name+"/"+parameter.name);
					if (parameter.type ==='FILEREAD' || parameter.type ==='FILEREADWRITE')
					{
						// console.log("calculateParameterUpload - Add
						// "+milkJob.name+"/"+parameter.name);
						var item = { "plugtour": milkJob.name, "id": milkJob.id, "parameter":parameter.name, "displayname" : milkJob.name+" ("+parameter.label+")"};
						item.internalid=milkJob.id+"#"+parameter.name;
						this.listParameterUpload.push( item );
						// set a default value ?
						// console.log("Set a default value?
						// "+this.parameterToUpload); // ||
						// this.parameterToUpload===null
						if (! this.parameterToUpload)
							this.parameterToUpload=item.internalid;
					}
				}
					
			}
			// console.log("calculateParameterUpload - end nbDetected="+this.listParameterUpload.length);
			
		}
		var me=this;
		me.sourceParameterToUpload=null;
		me.parameterToUploadStatus= " ";
		me.showUploadSuccess=false;

		$scope.$watch('files', function() {
			console.log("watch.start");

			if (! $scope.files)
			{
				console.log("watch : $filefile, stop");
				return;
			}			
			// find the parameters
			for (var i in me.listParameterUpload)
			{
				if (me.listParameterUpload[ i ].internalid == me.parameterToUpload)
					me.sourceParameterToUpload = me.listParameterUpload[ i ]; 
			}
			console.log("ParameterToUpload="+me.parameterToUpload);
			
			for (var i = 0; i < $scope.files.length; i++) {
				
				
				me.parameterToUploadStatus="Upload...";
				var file = $scope.files[i];
				me.inprogress=true;
				console.log("watch inProgress<=true upload file "+file);
			
				$scope.upload = $upload.upload({
					url: '/bonita/portal/fileUpload',
					method: 'POST',
					data: {myObj: $scope.myModelObj},
					file: file
				}).progress(function(evt) {
	// console.log('progress: ' + parseInt(100.0 * evt.loaded / evt.total) + '%
	// file :'+ evt.config.file.name);
				}).success(function(data, status, headers, config) {
					console.log('Watch: file is uploaded successfully. Response: ' + data + " Source="+me.sourceParameterToUpload);
					// console.log('SourceId='+me.sourceParameterToUpload.id);
					
					me.parameterToUploadStatus="Saving...";
					// now, upload it in the Tour
					var param={"id": me.sourceParameterToUpload.id, "parameter":me.sourceParameterToUpload.parameter, "file": data};	
					// console.log("Parameterupload="+ angular.toJson(param));
					var json = encodeURIComponent(angular.toJson(param, true));
					var d = new Date();		
					
					// console.log("watch CALL HTTP");
					$http.get( '?page=custompage_truckmilk&action=uploadParamFile&paramjson='+json+'&t='+d.getTime() )
					.success( function ( jsonResult, statusHttp, headers, config ) {
						
						// connection is lost ?
						if (statusHttp==401 || typeof jsonResult === 'string') {
							console.log("Redirected to the login page !");
							window.location.reload();
						}
						me.parameterToUploadStatus="  ";
						me.inprogress=false;
						me.showUploadSuccess=true;
						console.log("Watch.then HTTP inprogres<=false");
						
					})
					.error( function (jsonResult, statusHttp, headers, config) {	
						console.log("uploadParamFile.error HTTP statusHttp="+statusHttp);
						// connection is lost ?
						if (statusHttp==401) {
							console.log("Redirected to the login page !");
							window.location.reload();
						}

						me.inprogress=false;
						me.showUploadSuccess=true;
					});
				}); // end upload
			} // end loop file
			console.log("watch.end");

		});
		
		
	
			
		
		this.uploadParameterFile= function()
		{
			var self=this;
			self.inprogress=true;
		    // console.log("uploadParameterFile.start= inprogres<=true");
			var param={"plugintour": this.plugtour.id, "parametername":parameterdef.name, "filename":this.paramfile};

			var json = angular.toJson( param, false);
			
			// console.log("uploadParameterFile call HTTP");
			$http.get('?page=custompage_truckmilk&action=uploadParamFile&paramjson=' + json+'&t='+Date.now())
			.success( function ( jsonResult, statusHttp, headers, config ) {
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				self.inprogress=false;
			    // console.log("uploadParameterFile.receivedata HTTP - inprogress<=false result= "+angular.toJson(jsonResult, false));
			 	return self.list;
				},  function ( jsonResult ) {
					self.inprogress=false;
					// console.log("uploadParameterFile.error HTTP - inprogress<=false ");
			})
			.error(function(jsonResult, statusHttp, headers, config) {
				console.log("uploadParamFile.error HTTP statusHttp="+statusHttp);
				// connection is lost ?
				if (statusHttp==401) {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				self.inprogress=false;
				 
				
			 } );
		}
		
		// -----------------------------------------------------------------------------------------
		// Timer
		// -----------------------------------------------------------------------------------------
	
		var timerRefreshTime=120000;
		this.armTimer = function()
		{
			var self=this;
			console.log("armTimer: timerRefresh");
			$scope.timer = $timeout(function() { self.fireTimer() }, 120000);
		}
		this.fireTimer = function() {
			var self=this;
			console.log("FireTimer: autoRefresh="+this.autorefresh);
			
			if (this.autorefresh)
			{
				this.refresh( false );
			}
			// then rearm the timer
			$scope.timer = $timeout(function() { self.fireTimer() }, 120000); 
		}
		this.armTimer();
		
		// -----------------------------------------------------------------------------------------
		// Command adminstration
		// -----------------------------------------------------------------------------------------
		this.commandRedeploy = function() {
			var self=this;
			self.inprogress=true;
			self.listevents="";
			self.commandredeploy.deploimentsuc='';
			self.commandredeploy.deploimenterr='';
			self.commandredeploy.listevents = '';
			
			var param= { 'redeploydependencies' : this.commandredeploy.redeploydependencies};
			var json = encodeURIComponent(angular.toJson(param, true));
			var result = confirm("Do you want to redeploy the command?");
			if (result) {
				$http.get('?page=custompage_truckmilk&action=commandredeploy&paramjson=' + json+'&t='+Date.now())
				.success(function(jsonResult, statusHttp, headers, config) {
					
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();

					}
					self.inprogress = false;
					// console.log("commandRedeploy.receiveData HTTP inprogress<=false jsonResult="+angular.toJson(jsonResult));
	
					self.commandredeploy.status 		= jsonResult.status;
					self.commandredeploy.deploimentsuc	= jsonResult.deploimentsuc;
					self.commandredeploy.deploimenterr	= jsonResult.deploimenterr;
					self.commandredeploy.listevents		= jsonResult.listevents;

				}).error(function(jsonResult, statusHttp, headers, config) {
					console.log("commandRedeploy.error HTTP statusHttp="+statusHttp);
					// connection is lost ?
					if (statusHttp==401) {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					self.inprogress = false;
				});
			}	
		}
		this.Uninstall = function() {
			var result = confirm("Do you want to uninstall TruckMilk?");
			if (result) {
				var self=this;
				self.inprogress=true;
				self.listevents="";
			
				console.log("Uninstall confirmed");
				$http.get('?page=custompage_truckmilk&action=uninstall&t='+Date.now())
				.success(function(jsonResult, statusHttp, headers, config) {
					
					// connection is lost ?
					if (statusHttp==401 || typeof jsonResult === 'string') {
						console.log("Redirected to the login page !");
						window.location.reload();

					}
					self.inprogress = false;
					confirm("Uninstall confirmed. Page are not undeployed. Redirect to the administrator page, else the page will be re-installed");
					

				}).error(function(jsonResult, statusHttp, headers, config) {
					console.log("commandRedeploy.error HTTP statusHttp="+statusHttp);
					// connection is lost ?
					if (statusHttp==401) {
						console.log("Redirected to the login page !");
						window.location.reload();
					}
					self.inprogress = false;
				});
			}	
		}
		
		// -----------------------------------------------------------------------------------------
		// Scheduler
		// -----------------------------------------------------------------------------------------
		this.scheduler = { 'schedulerlistevents' : '', 'lastheartbeat':[], 'type':'', 'enable': false, listtypeschedulers:[]};
		
		
		this.schedulerOperation = function(action) {
			var self = this;
			self.inprogress = true;
			// console.log("schedulerOperation, inprogress=true");
			var param= { 'start' : action};
			var json = encodeURIComponent(angular.toJson(param, true));
			if (! self.scheduler)
				self.scheduler= {};
			
			self.scheduler.listevents='';
			
			// console.log("schedulerOperation call HTTP");
			$http.get('?page=custompage_truckmilk&action=scheduler&paramjson=' + json+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();

				}
				self.inprogress = false;
				// console.log("scheduler.receiveData HTTP inprogress<=false jsonResult="+angular.toJson(jsonResult));

				self.scheduler 						= jsonResult.scheduler;
				self.scheduler.listevents			= jsonResult.listevents;
				
			}).error(function(jsonResult, statusHttp, headers, config) {
				console.log("Scheduler.error HTTP statusHttp="+statusHttp);
				// connection is lost ?
				if (statusHttp==401) {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				self.inprogress = false;
			});
		}
		
		
		this.schedulerMaintenance = function(operation) {
			var self = this;
			self.inprogress = true;
			self.listevents='';

			// console.log("SchedulerMaintenance v2, Inprogress<=true operation=["+operation+"]");
			
			var param= {'operation' : operation,'reset' : true };
			if (this.scheduler) {
					param.newscheduler= this.scheduler.type;
					param.logheartbeat = this.scheduler.logheartbeat;
					param.nbsavedheartbeat = this.scheduler.nbsavedheartbeat;
			}
					
			var json = encodeURIComponent(angular.toJson(param, true));
			// that's means we already get the scheduler informatin
			if (self.scheduler) {
				self.scheduler.schedulerlistevents= '';
			}
			
			// console.log("schedulerMaintenance call HTTP");
			$http.get('?page=custompage_truckmilk&action=schedulermaintenance&paramjson=' + json+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				self.inprogress = false;
				
				// console.log("schedulerMaintenance.receiveData (scheduler)"+ angular.toJson(jsonResult,false));

				self.scheduler	= jsonResult.scheduler;
				self.scheduler.schedulerlistevents = jsonResult.listevents;
				
			}).error(function(jsonResult, statusHttp, headers, config) {
				console.log("SchedulerMaintenance.error HTTP statusHttp="+statusHttp);
				// connection is lost ?
				if (statusHttp==401) {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				
				self.inprogress = false;
			});
		}
		
		this.getListTypeScheduler = function()
		{
			// console.log("getListTypeScheduler:Start");
			if (this.scheduler)
				return this.scheduler.listtypeschedulers;
			return [];
		}
		
		// -----------------------------------------------------------------------------------------
		// Chart
		// -----------------------------------------------------------------------------------------
		this.getDashboardData = function() {
			var result=[ ["0", 51], ["1", 15], ["2", 7] ];
			return result;
		}
		this.getDashboardColumn = function() {
			return ["whattime", "value"];
		}
		// https://www.tutorialspoint.com/angular_googlecharts/angular_googlecharts_column_basic.htm
		this.getDashboardChart = function( mesure ) {
			var result1= {
				  "type": "ColumnChart",
				  "displayed": true,
				  "data": {
				    "cols": [
				      {
				        "type": "string",
				        "id": "whattime",
				        "label": "whattime"
				      },
				      {
				        "type": "number",
				        "id": "value",
				        "label": "Occurence"
				      }
				    ],
				    "rows": [
				      { "c": [ { "v": "00:00"}, { "v": 0 }] },
				      { "c": [ { "v": "00:10"}, { "v": 12 }]},
				      { "c": [ { "v": "00:20"}, { "v": 7 }]}
				  ]
				  }
			}
			var result= {
					  "type": "ColumnChart",
					  "displayed": true,
					  "columnNames": ["whattime", "value"],
					  "data": [ ["0", 5],
						  		["1", 15],
						  		["2", 7] ],
						"width":550,
						"height" : 400
				}
			
			return angular.fromJson( angular.toJson( result ));
			
			
			/*
			var data = new google.visualization.DataTable();
		    data.addColumn('string', 'Year');
		    data.addColumn('number', 'Score');
		    data.addRows([
		      ['2005',3.6],
		      ['2006',4.1],
		      ['2007',3.8],
		      ['2008',3.9],
		      ['2009',4.6]
		    ]);

		    barsVisualization = new google.visualization.ColumnChart(document.getElementById('mouseoverdiv'));
		    barsVisualization.draw(data, null);
		    */
			// return resultCopy;
		}
		
		// -----------------------------------------------------------------------------------------
		// tool
		// -----------------------------------------------------------------------------------------

		this.getHtml = function(listevents, sourceContext) {
			// console.log("getHtml:Start (r/o) source="+sourceContext);
			return $sce.trustAsHtml(listevents);
		}

		// -----------------------------------------------------------------------------------------
		// Delay Widget
		// -----------------------------------------------------------------------------------------
		this.getDelayScope = function( valueCompact )  {
			var res = valueCompact.split(":");
			return res[0];
		}
		this.getDelayValue = function( valueCompact )  {
			var res = valueCompact.split(":");
			return res[1];
		}
		this.compactDelay = function( variable, scope, value ) {
			variabl = scope+":"+value;
		}
		
		
		// -----------------------------------------------------------------------------------------
		// Thanks to Bonita to not implement the POST : we have to split the URL
		// -----------------------------------------------------------------------------------------
		var postParams=
		{
			"listUrlCall" : [],
			"action":"",
			"advPercent":0
			
		}
		this.sendPost = function(finalaction, json )
		{
			var self=this;
			self.inprogress=true;
			// console.log("sendPost inProgress<=true action="+finalaction+" Json="+ angular.toJson( json ));
			
			self.postParams={};
			self.postParams.listUrlCall=[];
			self.postParams.action= finalaction;
			var action = "collect_reset";
			// split the string by packet of 1800 (URL cut at 2800, and we have
			// to encode the string)
			while (json.length>0)
			{
				var jsonSplit= json.substring(0,1500);
				var jsonEncodeSplit = encodeURI( jsonSplit );
				
				// Attention, the char # is not encoded !!
				jsonEncodeSplit = jsonEncodeSplit.replace(new RegExp('#', 'g'), '%23');
				
				// console.log("collect_add JsonPartial="+jsonSplit);
				// console.log("collect_add JsonEncode ="+jsonEncodeSplit);
			
				
				self.postParams.listUrlCall.push( "action="+action+"&paramjsonpartial="+jsonEncodeSplit);
				action = "collect_add";
				json = json.substring(1500);
			}
			self.postParams.listUrlCall.push( "action="+self.postParams.action);
			
			
			self.postParams.listUrlIndex=0;
			self.executeListUrl( self ) // , self.listUrlCall, self.listUrlIndex
										// );
			// this.operationTour('updateJob', plugtour, plugtour, true);
			// console.log("sendPost.END")
			
		}
		
		this.executeListUrl = function( self ) // , listUrlCall, listUrlIndex )
		{
			// console.log(" CallList "+self.postParams.listUrlIndex+"/"+ self.postParams.listUrlCall.length+" : "+self.postParams.listUrlCall[ self.postParams.listUrlIndex ]);
			self.postParams.advPercent= Math.round( (100 *  self.postParams.listUrlIndex) / self.postParams.listUrlCall.length);
			
			// console.log("executeListUrl call HTTP");

			$http.get( '?page=custompage_truckmilk&t='+Date.now()+'&'+self.postParams.listUrlCall[ self.postParams.listUrlIndex ] )
			.success( function ( jsonResult, statusHttp, headers, config ) {
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				// console.log("executeListUrl receive data HTTP");
				// console.log("Correct, advance one more",
				// angular.toJson(jsonResult));
				self.postParams.listUrlIndex = self.postParams.listUrlIndex+1;
				if (self.postParams.listUrlIndex  < self.postParams.listUrlCall.length )
					self.executeListUrl( self ) // , self.listUrlCall,
												// self.listUrlIndex);
				else
				{
					self.inprogress = false;
					// console.log("sendPost finish inProgress<=false jsonResult="+ angular.toJson(jsonResult));

					self.postParams.advPercent= 100; 
	
					if (self.postParams.action=="updateJob")
					{
						self.listeventsexecution    		= jsonResult.listevents;
						
						// bob update the current job
						milkJobSelf 						= jsonResult.milkjob;
						// ok, update the name of this job (name may have change)
						console.log("Update nname milkJob="+milkJobSelf);
						for (var i in self.listplugtour) {
							var localPlugTour = self.listplugtour[ i ];
							console.log("Compare local ["+localPlugTour.id+"]");
							console.log(" with received["+ milkJobSelf.id+"]");
							if (localPlugTour.id === milkJobSelf.id) {
								console.log("Found it update local name["+localPlugTour.name+"]");
								
								localPlugTour.name = milkJobSelf.name;
							}
						}
					
						
						// self.refreshPlugTourFromServer(false, self, jsonResult ); 

					}
					if (self.postParams.action=="testButton")
					{
						self.currentButtonExecution.listeventsexecution =  jsonResult.listevents;;			
					}
					
				}
			})
			.error( function(jsonResult, statusHttp, headers, config) {
				console.log("executeListUrl.error HTTP statusHttp="+statusHttp);
				// connection is lost ?
				if (statusHttp==401) {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				self.inprogress = false;				
				});	
		};
		
		// -----------------------------------------------------------------------------------------
		// init
		// -----------------------------------------------------------------------------------------

		this.loadinit();

	
	});

})();