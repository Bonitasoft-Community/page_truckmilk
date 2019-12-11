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
	appCommand.controller('TruckMilkControler', function($http, $scope, $sce, $timeout, $upload) {

		this.showhistory = function(show) {
			this.isshowhistory = show;
		}
		this.navbaractiv='Jobs';
		
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
			return '';
		}
		
		
		this.inprogress = false;

		this.listplugtour =[];
		this.listplugtourSimul = [ {
			'name' : 'Replay process Free',
			'plugin' : 'ReplayFailedTask',
			'description' : 'Replay Failed Task at intervalle',
			'state' : 'ACTIF',
			'frequency' : 'Every 5 minutes',
			'show' : {
				'schedule' : false,
				'parameters' : false,
				'report' : false,
				'analysis' : false
			},
			'parameters' : [],
			'schedule' : {},
			'report' : ''
		} ];
		this.listplugin = [];
		this.newtourname = '';
		this.refreshDate=null;
		this.scheduler={ 'enable': false, listtypeschedulers:[]};
		
		// a new listPlugTour is receive : refresh all what we need in the
		// interface
		this.afterRefreshListPlugTour = function() {
			// console.log("afterRefreshListPlugTour:Start");
			for ( var i in this.listplugtour) {
				var plugtourindex = this.listplugtour[i];
				this.preparePlugTourParameter(plugtourindex);
			}
			// calculate a uniq time stamp, to avoid the infinit loop when
			// calculate the downloadParameterFile
			this.refreshDate = new Date();
			
			this.calculateParameterUpload();
		}

		
		// -----------------------------------------------------------------------------------------
		// getListPlugin
		// -----------------------------------------------------------------------------------------

		this.getListPlugIn = function() {
			// console.log("getListPlugIn:Start (r/o)");
			return this.listplugin;
		}
		this.getListPlugTour = function() {
			// consoleCall HTTPgTour:Start (r/o)");
			return this.listplugtour;
		}

		this.loadinit = function() {
			var self = this;
			
			self.inprogress = true;
			self.showUploadSuccess=false;

			console.log("loadinit inprogress<=true. Call now");
			self.listevents='';
			
			// console.log("loadinit Call HTTP");
			$http.get('?page=custompage_truckmilk&action=startup&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}

				console.log("loadinit.receiveData HTTP");
				
				self.listplugtour 	= jsonResult.listplugtour;
				self.listplugin 	= jsonResult.listplugin;
				self.scheduler 		= jsonResult.scheduler;
				self.listevents 	= jsonResult.listevents;
				self.deploimentsuc  = jsonResult.deploimentsuc;
				self.deploimenterr  = jsonResult.deploimenterr;
				
				console.log("loadInit start initialisation");
				// prepare defaut value
				self.afterRefreshListPlugTour();

				self.inprogress = false;
				console.log("loadinit.end inprogress<=false");

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
			console.log("refresh.start listplugtour="+angular.toJson( self.listplugtour )); 
			if (self.inprogress)
			{
				console.log("Refresh in progress, skip this one");
				return;
			}
			
			var verb="refresh";
			if (completeStatus)
				verb="getstatus";
			
			self.completeStatus = completeStatus;
			self.listevents='';
			self.inprogress = true;
			self.showUploadSuccess=false;

			console.log("refresh: inprogress<=true. Call now 2.2");
			// console.log("refresh call HTTP");
			$http.get('?page=custompage_truckmilk&action='+verb+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				self.inprogress = false;
				console.log("success.then HTTP inprogress<=true, status="+statusHttp);
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}

					
				self.deploiment = jsonResult.deploiment;
				if (self.completeStatus)
				{
					self.listplugtour 	= jsonResult.listplugtour;
					self.afterRefreshListPlugTour();
					self.scheduler 		= jsonResult.scheduler;

				}
				else
				{
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
								// console.log("Found it !");
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
								
							}

						}
						if (! foundIt)
						{
							// add it in the list
							// console.log("Found it !");
							// self.listplugtour.push(serverPlugTour );
						}
					}						
				}
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
		this.addJob = function(plugin, nameJob) {
			console.log("addJob: add["+nameJob+"]");
			if (! nameJob || nameJob === '')
			{
				alert("Name is mandatory");
				return;
			}
			var param = {
				'plugin' : plugin,
				'name' : nameJob
			};
			this.operationJob('addJob', param, null, true);
		}

		this.removeJob = function(plugtour) {
			var result = confirm("Do you want to remove this job?");
			if (result) {
				console.log("removeJob:Start");

				var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};
				this.operationJob('removeJob', param, null, false);
			}
		}

		this.abortJob = function( plugtour) {
			console.log("abortTour:Start");
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};
			this.operationJob('abortJob', param, plugtour, false);
		}

		<!-- Activate / Deactivate -->
		this.activateJob = function(plugtour) {
			console.log("startJob:Start");
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};
			this.operationJob('activateJob', param, plugtour, false);
		}
		
		this.deactivateJob = function(plugtour) {
			console.log("stopJob:beginning");
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};

			this.operationJob('deactivateJob', param, plugtour, true);		
		}

		
		this.updateJob= function(plugtour) {
			console.log("updateJob START")
			var paramStart = plugtour;
			var self=this;
			// update maybe very heavy : truncate it
			self.listeventsexecution="";
			plugtour.listevents='';
			// first action will be a reset
			// prepare the string
			var  plugtourcopy =angular.copy( plugtour );
			plugtourcopy.parametersdef = null; // parameters are in
												// plugtourcopy.parametersvalue
			plugtourcopy.parameters = null;
			plugtourcopy.lastexecutionlistevents=null;
			plugtourcopy.savedExecution = null;
			
			var json = angular.toJson( plugtourcopy, false);
			
			self.sendPost('updateJob', json );
		}
		
		
		
		this.immediateJob = function(plugtour) {
			console.log("immediateJob:Start");
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};

			this.operationJob('immediateExecution', param, plugtour, true);		
		}
		this.abortJob = function(plugtour) {
			console.log("abortJob:begin");
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};

			this.operationJob('abortJob', param, plugtour, true);		
		}
		this.resetJob = function(plugtour) {
			console.log("resetJob:begin");
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};

			this.operationJob('resetJob', param, plugtour, true);		
		}
		
		// execute an operation on Job
		this.operationJob = function(action, param, plugtour, refreshParam) {
			console.log("operationJob:Start");

			
			var self = this;
			self.inprogress = true;
			console.log("operationJob START ["+action+"] inprogress<=true");

			self.action = action;
			self.addlistevents= "";
			self.listevents = "";
			self.currentplugtour = plugtour;
			self.refreshParam = refreshParam;
			var json = encodeURIComponent(angular.toJson(param, true));
			self.listevents='';
			// console.log("operationJob Call HTTP");

			$http.get('?page=custompage_truckmilk&action=' + action + '&paramjson=' + json+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				self.inprogress = false;
				console.log("operationJob.receiveData HTTP inprogress<=false");
				
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				
				if (jsonResult.listplugtour != undefined)
				{
					// todo keep the one open
					self.listplugtour = jsonResult.listplugtour;
					self.afterRefreshListPlugTour();
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
					self.afterRefreshListPlugTour();
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

		// preparePlugTourParameter
		// we copy the different parameters from parameters[SourceRESTAPI] to
		// parametersvalue [ANGULAR MAPPED]
		this.preparePlugTourParameter = function(plugtour) {
			
			// console.log("preparePlugTourParameter.start: " + plugtour.name);
			plugtour.newname=plugtour.name;
			plugtour.parametersvalue = {};
			
			for ( var key in plugtour.parameters) {
				plugtour.parametersvalue[key] = JSON.parse( JSON.stringify( plugtour.parameters[key]) );
				// console.log("Parameter[" + key + "] value[" + angular.toJson(
				// plugtour.parameters[key]) + "] ="+angular.toJson(
				// plugtour.parametersvalue,true ));
				
				
			
			}
			// prepare all test button
			for (var key in plugtour.parametersdef)
			{
				if (plugtour.parametersdef[ key ].type==="BUTTONARGS")
				{
					var buttonName=plugtour.parametersdef[ key ].name;
					// console.log("buttonARGS detected key=["+key+"]
					// name=["+buttonName+"]
					// args="+angular.toJson(plugtour.parametersdef[ key
					// ].args));
					// set the default value
					var listArgs=plugtour.parametersdef[ key ].args;
					var mapArgsValue={};
					plugtour.parametersvalue[ buttonName ]=mapArgsValue;
					for (var i in listArgs)
					{
						var argname=listArgs[ i ].name;
						mapArgsValue[ argname ] = listArgs[ i ].value;
					}
				}
			}
			console.log("PlugTourPreration END " + plugtour.name+" plugtour.parametersvalue="+angular.toJson( plugtour.parametersvalue,true ));

		}
		
		// -----------------------------------------------------------------------------------------
		// manupulate array in parameters
		this.isAnArray = function( value ) {
			console.log("isAnArray:Start");
			// console.log("operationTour Is An Array");
		    return Array.isArray( value );
	     }
		this.addInArray = function( valueArray, valueToAdd )
		{
			console.log("addInArray:Start");
			// console.log("addInArray valueArray="+angular.toJson( valueArray
			// ));
			valueArray.push(  valueToAdd );
			// console.log("add valueArray="+angular.toJson( valueArray ));
		}
		this.removeInArray = function( valueArray, value)
		{
			console.log("removeInArray:Start");
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
			console.log("getArgs:Start");
			 return new Array(parameterdef.nbargs);  
		}
		
		// click on a test button
		var currentButtonExecution=null;
		this.testbutton = function(parameterdef, plugtour)
		{
			console.log("testbutton:Start");
			var self=this;
			var  plugtourcopy =angular.copy( plugtour );
			plugtourcopy.parametersdef = null; // parameters are in
												// plugtourcopy.parametersvalue
			plugtourcopy.parameters = null;
			plugtourcopy.savedExecution = null;
			plugtourcopy.buttonName= parameterdef.name;
			plugtourcopy.args = plugtour.parametersvalue[ parameterdef.name ];
			
			// console.log("testbutton with "+angular.toJson( plugtourcopy ));
			parameterdef.listeventsexecution="";
			self.currentButtonExecution = parameterdef;	
			parameterdef.listeventsexecution="";
			self.sendPost('testButton', angular.toJson( plugtourcopy ) );
		}
				
		// -----------------------------------------------------------------------------------------
		// parameters tool
		// -----------------------------------------------------------------------------------------
		this.query = function(queryName, textSearch, parameterDef) {
			console.log("query:Start");
			var self=parameterDef;
			self.inprogress=true;
			console.log("operationTour.query ["+queryName+"] on ["+textSearch+"] inprogress<=true");

			var param={ 'userfilter' :  textSearch};
			
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
				console.log("Query.receiveData HTTP inProgress<=false result="+angular.toJson(jsonResult.data, false));
			 	self.list =  jsonResult.data.listProcess;
		
				return self.list;
			}, function ( jsonResult ) {
				console.log("QueryUser HTTP THEN");
				self.inprogress=false;

			});

		  };
		  
		// -----------------------------------------------------------------------------------------
		// Show information
		// -----------------------------------------------------------------------------------------
		this.hideall = function(plugtour) {
			console.log("hideall:Start");
			plugtour.show = {
				'schedule' : false,
				'parameters' : false,
				'report' : false,
				'analysis' : false,
				'hostrestriction':false
			};
		}

		this.showSchedule = function(plugtour) {
			console.log("showSchedule:Start");
			this.hideall(plugtour);
			plugtour.show.schedule = true;
		}
		this.showHostRestriction = function(plugtour) {
			console.log("showHost Restriction:Start");
			this.hideall(plugtour);
			plugtour.show.hostrestriction = true;
		}

		this.showparameters = function(plugtour) {
			// console.log("showparameters:Start");

			this.hideall(plugtour);
			plugtour.show.parameters = true;
		}
		this.showreport = function(plugtour) {
			// console.log("showreport:Start");
			this.hideall(plugtour);
			plugtour.show.report = true;
		}
		this.showanalysis = function(plugtour) {
			// console.log("showreport:Start");
			this.hideall(plugtour);
			plugtour.show.analysis = true;
		}
		this.hasanalysis = function( plugtour) {
			return plugtour.analysisdef.length > 0 ;
		}
		this.getJobStyle= function(plugtour) {
			// console.log("getTourStyle:Start (r/o) plugtour="+plugtour.id+"
			// imediateExecution="+plugtour.imediateExecution+"
			// inExecution="+plugtour.trackExecution.inExecution+"
			// enable="+plugtour.enable+" askForStop="+plugtour.askForStop);
			
			/*
			 * no more color if (plugtour.imediateExecution) return
			 * "background-color: #fafabd80"; if (plugtour.inExecution &&
			 * plugtour.askForStop) return "background-color: #ffa5004f"; if
			 * (plugtour.inExecution) return "background-color: #f6f696";
			 * 
			 * if (plugtour.lastexecutionstatus == "ERROR") return
			 * "background-color: #e7c3c3"; if (plugtour.lastexecutionstatus ==
			 * "WARNING") return "background-color: #fcf8e3"; if
			 * (plugtour.enable) return "background-color: #ebfae5";
			 */
		}
		this.getNowStyle= function( plugtour )
		{
			// console.log("getNowStyle:Start (r/o) plugtour="+plugtour.id);
			if (plugtour.trackExecution.imediateExecution)
				return "btn btn-success btn-xs"
			return "btn btn-info btn-xs";
		}		
		this.getNowTitle=function( plugtour )
		{
			// console.log("getNowTitle:Start (r/o) plugtour="+plugtour.id);
			if (plugtour.trackExecution.imediateExecution)
				return "An execution will start as soon as possible, at the next round"
			return "Click to have an immediat execution (this does not change the schedule, or the activate state)";
		}
		
		this.getAbortStyle= function( plugtour )
		{
			return "btn btn-warning btn-xs"
		}		
		this.getAbortTitle=function( plugtour )
		{
			return "Click to ask the job to stop as soon as possible, at the end of it's current transaction";
		}
		this.getResetStyle= function( plugtour )
		{
			return "btn btn-danger btn-xs"
		}		
		this.getResetTitle=function( plugtour )
		{
			return "Click to kill immediately the job";
		}
		
		// -----------------------------------------------------------------------------------------
		// Report parameters
		// -----------------------------------------------------------------------------------------
		this.downloadParameterFile = function(plugtour, parameterdef)
		{
			// console.log("downloadParameterFile:Start (r/o)
			// ["+parameterdef.name+"]");
			var param={"plugintour": plugtour.id, "parametername":parameterdef.name};			
			var json = encodeURIComponent(angular.toJson(param, true));
			// do not calculate a new date now:we will have a recursive call in
			// Angular
			return "?page=custompage_truckmilk&action=downloadParamFile&paramjson=" + json+'&t='+this.refreshDate;
		}
		
		
		// calculate listParameterUpload
		this.listParameterUpload=[];
		this.parameterToUpload=null;
		
		this.calculateParameterUpload = function() {
			// console.log("calculateParameterUpload - start");
			this.listParameterUpload = [];
			for ( var i in this.listplugtour) {
				var plugtour = this.listplugtour[i];
				// console.log("calculateParameterUpload - look
				// "+plugtour.name);
				for ( var key in plugtour.parametersdef) {
					var parameter = plugtour.parametersdef[ key ];
					// console.log("calculateParameterUpload - look
					// "+plugtour.name+"/"+parameter.name);
					if (parameter.type ==='FILEREAD' || parameter.type ==='FILEREADWRITE')
					{
						// console.log("calculateParameterUpload - Add
						// "+plugtour.name+"/"+parameter.name);
						var item = { "plugtour": plugtour.name, "id": plugtour.id, "parameter":parameter.name, "displayname" : plugtour.name+" ("+parameter.label+")"};
						item.internalid=plugtour.id+"#"+parameter.name;
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
			console.log("calculateParameterUpload - end nbDetected="+this.listParameterUpload.length);
			
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
					console.log('SourceId='+me.sourceParameterToUpload.id);
					
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
		    console.log("uploadParameterFile.start= inprogres<=true");
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
			    console.log("uploadParameterFile.receivedata HTTP - inprogress<=false result= "+angular.toJson(jsonResult, false));
			 	return self.list;
				},  function ( jsonResult ) {
				self.inprogress=false;
			    console.log("uploadParameterFile.error HTTP - inprogress<=false ");
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
					console.log("commandRedeploy.receiveData HTTP inprogress<=false jsonResult="+angular.toJson(jsonResult));
	
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
		
		// -----------------------------------------------------------------------------------------
		// Scheduler
		// -----------------------------------------------------------------------------------------
		this.schedulerOperation = function(action) {
			var self = this;
			self.inprogress = true;
			console.log("schedulerOperation, inprogress=true");
			var param= { 'start' : action};
			var json = encodeURIComponent(angular.toJson(param, true));
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
				console.log("scheduler.receiveData HTTP inprogress<=false jsonResult="+angular.toJson(jsonResult));

				self.scheduler.status 		= jsonResult.status;
				self.scheduler.listevents	= jsonResult.listevents;
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

			console.log("SchedulerMaintenance v2, Inprogress<=true operation=["+operation+"]");
			var param= { 'operation': operation, 'newscheduler': this.scheduler.selectscheduler, 'reset':true };
			var json = encodeURIComponent(angular.toJson(param, true));
			self.scheduler = { 'schedulerlistevents' : '' };
			
			// console.log("schedulerMaintenance call HTTP");
			$http.get('?page=custompage_truckmilk&action=schedulermaintenance&paramjson=' + json+'&t='+Date.now())
			.success(function(jsonResult, statusHttp, headers, config) {
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				self.inprogress = false;
				console.log("schedulerMaintenance.receiveData HTTP Finish inprogress<=false"+ angular.toJson(jsonResult,false));

				self.scheduler	= jsonResult;
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
		// tool
		// -----------------------------------------------------------------------------------------

		this.getHtml = function(listevents, sourceContext) {
			// console.log("getHtml:Start (r/o) source="+sourceContext);
			return $sce.trustAsHtml(listevents);
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
			console.log("sendPost inProgress<=true action="+finalaction+" Json="+ angular.toJson( json ));
			
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
			console.log("sendPost.END")
			
		}
		
		this.executeListUrl = function( self ) // , listUrlCall, listUrlIndex )
		{
			console.log(" CallList "+self.postParams.listUrlIndex+"/"+ self.postParams.listUrlCall.length+" : "+self.postParams.listUrlCall[ self.postParams.listUrlIndex ]);
			self.postParams.advPercent= Math.round( (100 *  self.postParams.listUrlIndex) / self.postParams.listUrlCall.length);
			
			// console.log("executeListUrl call HTTP");

			$http.get( '?page=custompage_truckmilk&t='+Date.now()+'&'+self.postParams.listUrlCall[ self.postParams.listUrlIndex ] )
			.success( function ( jsonResult, statusHttp, headers, config ) {
				// connection is lost ?
				if (statusHttp==401 || typeof jsonResult === 'string') {
					console.log("Redirected to the login page !");
					window.location.reload();
				}
				console.log("executeListUrl receive data HTTP");
				// console.log("Correct, advance one more",
				// angular.toJson(jsonResult));
				self.postParams.listUrlIndex = self.postParams.listUrlIndex+1;
				if (self.postParams.listUrlIndex  < self.postParams.listUrlCall.length )
					self.executeListUrl( self ) // , self.listUrlCall,
												// self.listUrlIndex);
				else
				{
					self.inprogress = false;
					console.log("sendPost finish inProgress<=false jsonResult="+ angular.toJson(jsonResult));

					self.postParams.advPercent= 100; 
	
					if (self.postParams.action=="updateJob")
					{
						self.listeventsexecution    		= jsonResult.listevents;
						self.listeventsconfig 				= jsonResult.listeventsconfig;
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