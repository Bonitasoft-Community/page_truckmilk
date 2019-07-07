'use strict';
/**
 * 
 */

(function() {

	var appCommand = angular.module('truckmilk', [ 'googlechart', 'ui.bootstrap','ngMaterial' ]);

	// --------------------------------------------------------------------------
	//
	// Controler TruckMilk
	//
	// --------------------------------------------------------------------------

	// Ping the server
	appCommand.controller('TruckMilkControler', function($http, $scope, $sce, $timeout) {

		this.showhistory = function(show) {
			this.isshowhistory = show;
		}
		this.navbaractiv='Tour';
		
		this.getNavClass = function( tabtodisplay )
		{
			if (this.navbaractiv === tabtodisplay)
			 return 'ng-isolate-scope active';
			return 'ng-isolate-scope';
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
				'report' : false
			},
			'parameters' : [],
			'schedule' : {},
			'report' : ''
		} ];
		this.listplugin = [];
		this.newtourname = '';
		
		this.scheduler={ 'enable': false, listtypeschedulers:[]};
		
		// -----------------------------------------------------------------------------------------
		// getListPlugin
		// -----------------------------------------------------------------------------------------

		this.getListPlugIn = function() {
			return this.listplugin;
		}
		this.getListPlugTour = function() {
			return this.listplugtour;
		}

		this.loadinit = function() {
			var self = this;
			self.inprogress = true;
			self.listevents='';
			
			$http.get('?page=custompage_truckmilk&action=startup&t='+Date.now()).success(function(jsonResult) {
				console.log("history", jsonResult);
				self.inprogress = false;

				self.listplugtour 	= jsonResult.listplugtour;
				self.listplugin 	= jsonResult.listplugin;
				self.scheduler 		= jsonResult.scheduler;
				self.listevents 	= jsonResult.listevents;
				self.deploimentsuc  = jsonResult.deploimentsuc;
				self.deploimenterr  = jsonResult.deploimenterr;
				
				console.log("TruckMilk RECEPTION INIT");
				// prepare defaut value
				for ( var i in self.listplugtour) {
					var plugtourindex = self.listplugtour[i];
					self.preparePlugTourParameter(plugtourindex);
				}
				
				console.log("TruckMilk END init ");
				
			}).error(function() {

				self.inprogress = false;

				self.listplugtour = [];
				self.listplugin = [];

			});

		}

		this.autorefresh=false;
		this.completeStatus=false;
		
		
		// completeStatus : refresh the complete list + getStatus on scheduler, else refresh only some attributes
		this.refresh = function( completeStatus ) {
			console.log("truckmilk:completeStatus ["+completeStatus+"]");

			var self = this;
			self.deploimentsuc=''; // no need to keep it for ever
			console.log("truckmilk: refresh listplugtour="+angular.toJson( self.listplugtour )); 
			
			
			// rearm the timer
			$scope.timer = $timeout(function() { self.refresh( false ) }, 120000);
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
			
			$http.get('?page=custompage_truckmilk&action='+verb+'&t='+Date.now()).success(function(jsonResult) {
				// console.log("history", jsonResult);
				self.inprogress = false;
				self.deploiment = jsonResult.deploiment;
				if (self.completeStatus)
				{
					self.listplugtour 	= jsonResult.listplugtour;
					// prepare defaut value
					for ( var i in self.listplugtour) {
						var plugtourindex = self.listplugtour[i];
						self.preparePlugTourParameter(plugtourindex);
					}
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
							console.log("Compare server["+serverPlugTour.name+"]("+serverPlugTour.id+") with ["+localPlugTour.name+"]("+localPlugTour.id+")");
							if (serverPlugTour.id == localPlugTour.id)
							{
								console.log("Found it !");
								foundIt= true;
								localPlugTour.enable					= serverPlugTour.enable;
								// localPlugTour.name		= serverPlugTour.name;
								// cron
								localPlugTour.nextexecutionst			= serverPlugTour.nextexecutionst;
								localPlugTour.lastexecutionstatus		= serverPlugTour.lastexecutionstatus;
								localPlugTour.lastexecutionlistevents	= serverPlugTour.lastexecutionlistevents;
							}

						}
						if (! foundIt)
						{
							// add it in the list
							console.log("Found it !");
							// self.listplugtour.push(serverPlugTour );
						}
					}						
				}
				self.listplugin 	= jsonResult.listplugin;
				self.listevents 	= jsonResult.listevents;
				

			}).error(function() {

				self.inprogress = false;

				self.listplugtour = [];
				self.listplugin = [];

			});

		}
		this.addTour = function(plugin, nameTour) {
			var param = {
				'plugin' : plugin,
				'name' : nameTour
			};
			this.operationTour('addTour', param, null, true);
		}

		this.removeTour = function(plugtour) {
			console.log("removeTour");
			var param = {
				'name' :  plugtour.name,
				'id'   :  plugtour.id
			};
			this.operationTour('removeTour', param, null, false);
		}

		this.stopTour = function( plugtour) {
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};
			this.operationTour('stopTour', param, plugtour, false);
		}
		this.startTour = function(plugtour) {
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};
			this.operationTour('startTour', param, plugtour, false);
		}
		
		
		
		this.updatePlugtour= function(plugtour) {
			console.log("UpdatePlugtour START")
			var paramStart = plugtour;
			var self=this;
			// update maybe very heavy : truncate it
			self.listeventsexecution="";
			plugtour.listevents='';
			// first action will be a reset
			// prepare the string
			var  plugtourcopy =angular.copy( plugtour );
			plugtourcopy.parametersdef = null; // parameters are in plugtourcopy.parametersvalue
			plugtourcopy.parameters = null;
			plugtourcopy.lastexecutionlistevents=null;
			
			var json = angular.toJson( plugtourcopy, false);
			
			self.sendPost('updateTour', json );
		}
		
		
		
		this.immediateTour = function(plugtour) {
			var param = {
					'name' :  plugtour.name,
					'id'   :  plugtour.id
				};

			this.operationTour('immediateExecution', param, plugtour, true);		
		}
		
		// execute an operation on tour
		this.operationTour = function(action, param, plugtour, refreshParam) {
			console.log("operationTour START ["+action+"]");
			
			var self = this;
			self.action = action;
			self.addlistevents= "";
			self.listevents = "";
			self.inprogress = true;
			self.currentplugtour = plugtour;
			self.refreshParam = refreshParam;
			var json = encodeURIComponent(angular.toJson(param, true));
			self.listevents='';
			
			$http.get('?page=custompage_truckmilk&action=' + action + '&paramjson=' + json+'&t='+Date.now()).success(function(jsonResult) {
				console.log("TruckMilk - operation tour Result:", jsonResult);
				self.inprogress = false;

				if (jsonResult.listplugtour != undefined)
				{
					// todo keep the one open
					self.listplugtour = jsonResult.listplugtour;
					self.refreshParam=true; // force it to true
				}
				if (jsonResult.enable != undefined && self.currentplugtour != undefined)
				{	
					console.log("enable=", jsonResult.enable);
					self.currentplugtour.enable = jsonResult.enable;
				}

				if (self.currentplugtour != null) {
					self.currentplugtour.listevents = jsonResult.listevents;
				}
				else if (self.action==='addTour') {
					self.addlistevents = jsonResult.listevents;
				}
				else {
					self.listevents = jsonResult.listevents;
				}
				if (self.refreshParam) {
					
					console.log("TruckMilk RECEPTION TOUR : Refresh them");
					// prepare defaut value
					for ( var i in self.listplugtour) {
						var plugtourindex = self.listplugtour[i];
						self.preparePlugTourParameter(plugtourindex);
					}
				}
			}).error(function() {

				self.inprogress = false;

				self.listplugtour = [];
				self.listplugin = [];

			});

		}

		// preparePlugTourParameter
		// we copy the different parameters from parameters[SourceRESTAPI] to parametersvalue [ANGULAR MAPPED] 
		this.preparePlugTourParameter = function(plugtour) {
			console.log("PlugTourPreration START" + plugtour.name);
			plugtour.newname=plugtour.name;
			plugtour.parametersvalue = {};
			
			for ( var key in plugtour.parameters) {
				plugtour.parametersvalue[key] = JSON.parse( JSON.stringify( plugtour.parameters[key]) );
				// console.log("Parameter[" + key + "] value[" + angular.toJson( plugtour.parameters[key]) + "] ="+angular.toJson( plugtour.parametersvalue,true ));
				
				
			
			}
			// prepare all test button
			for (var key in plugtour.parametersdef)
			{
				if (plugtour.parametersdef[ key ].type==="BUTTONARGS")
				{
					var buttonName=plugtour.parametersdef[ key ].name;
					// console.log("buttonARGS detected key=["+key+"] name=["+buttonName+"] args="+angular.toJson(plugtour.parametersdef[ key ].args));
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
			console.log("operationTour Is An Array");

		     return Array.isArray( value );
	     }
		this.addInArray = function( valueArray, valueToAdd )
		{
			console.log("addInArray valueArray="+angular.toJson( valueArray ));
			valueArray.push(  valueToAdd );
			// console.log("add valueArray="+angular.toJson( valueArray ));
		}
		this.removeInArray = function( valueArray, value)
		{
			console.log("removeInArray value "+value);
			var index = valueArray.indexOf(value);
			if (index > -1) {
				valueArray.splice(index, 1);
			}
			console.log("remove value ["+value+"] index ["+index+"] valueArray="+angular.toJson( valueArray ));
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
		this.testbutton = function(parameterdef, plugtour)
		{
			var self=this;
			var  plugtourcopy =angular.copy( plugtour );
			plugtourcopy.parametersdef = null; // parameters are in plugtourcopy.parametersvalue
			plugtourcopy.parameters = null;
			plugtourcopy.buttonName= parameterdef.name;
			plugtourcopy.args = plugtour.parametersvalue[ parameterdef.name ];
			
			console.log("testbutton with "+angular.toJson( plugtourcopy ));
			parameterdef.listeventsexecution="";
			self.currentButtonExecution = parameterdef;	
			parameterdef.listeventsexecution="";
			self.sendPost('testButton', angular.toJson( plugtourcopy ) );
		}
				
		// -----------------------------------------------------------------------------------------
		// parameters tool
		// -----------------------------------------------------------------------------------------
		this.query = function(queryName, textSearch, parameterDef) {
			var self=parameterDef;
			console.log("Query ["+queryName+"] on ["+textSearch+"]");
			self.inprogress=true;
			var param={ 'userfilter' :  textSearch};
			
			var json = encodeURI( angular.toJson( param, false));
			// 7.6 : the server force a cache on all URL, so to bypass the cache, then create a different URL
			var d = new Date();
			
			return $http.get( '?page=custompage_truckmilk&action='+queryName+'&paramjson='+json+'&t='+d.getTime() )
			.then( function ( jsonResult ) {
				console.log("Query - result= "+angular.toJson(jsonResult, false));
					self.inprogress=false;
				 	self.list =  jsonResult.data.listProcess;
			
					return self.list;
					},  function ( jsonResult ) {
					console.log("Queryprocess THEN");
					self.inprogress=false;
			});

		  };
		  
		// -----------------------------------------------------------------------------------------
		// Show information
		// -----------------------------------------------------------------------------------------
		this.hideall = function(plugtour) {
			plugtour.show = {
				'schedule' : false,
				'parameters' : false,
				'report' : false
			};
		}

		this.showSchedule = function(plugtour) {
			this.hideall(plugtour);
			plugtour.show.schedule = true;
		}

		this.showparameters = function(plugtour) {

			this.hideall(plugtour);
			plugtour.show.parameters = true;
		}
		this.showreport = function(plugtour) {
			this.hideall(plugtour);
			plugtour.show.report = true;
		}

		this.getTourStyle= function(plugtour) {
			if (plugtour.lastexecutionstatus == "ERROR")
				return "background-color: #e7c3c3";
			if (plugtour.lastexecutionstatus == "WARNING")
				return "background-color: #fcf8e3";
		}
		this.getNowStyle= function( plugtour )
		{
			if (plugtour.imediateExecution)
				return "btn btn-success btn-xs"
			return "btn btn-primary btn-xs";
		}
		this.getNowTitle=function( plugtour )
		{
			if (plugtour.imediateExecution)
				return "An execution will start as soon as possible, at the next round"
			return "Click to have an immediat execution (this does not change the schedule, or the activate state)";
		}
		
		// -----------------------------------------------------------------------------------------
		// Timer
		// -----------------------------------------------------------------------------------------
	
		this.armTimer = function()
		{
			var self=this;
			console.log("truckMilk:arm timeout");
			$scope.timer = $timeout(function() { self.refresh( false ) }, 120000);
		}
		this.armTimer();
			
		// -----------------------------------------------------------------------------------------
		// Scheduler
		// -----------------------------------------------------------------------------------------
		this.schedulerOperation = function(action) {
			var self = this;
			self.inprogress = true;
			var param= { 'start' : action};
			var json = encodeURIComponent(angular.toJson(param, true));
			self.scheduler.listevents='';
			
			$http.get('?page=custompage_truckmilk&action=scheduler&paramjson=' + json+'&t='+Date.now()).success(function(jsonResult) {
				console.log("scheduler", jsonResult);
				self.inprogress = false;

				self.scheduler.status 		= jsonResult.status;
				self.scheduler.listevents	= jsonResult.listevents;
			}).error(function() {

				self.inprogress = false;

				
			});
		}
		
		
		this.schedulerMaintenance = function(operation) {
			var self = this;
			self.inprogress = true;
			var param= { 'operation': operation, 'newscheduler': this.scheduler.selectscheduler };
			var json = encodeURIComponent(angular.toJson(param, true));
			self.scheduler.schedulerlistevents='';

			$http.get('?page=custompage_truckmilk&action=status&paramjson=' + json+'&t='+Date.now()).success(function(jsonResult) {
				console.log("scheduler", jsonResult);
				self.inprogress = false;

				self.scheduler	= jsonResult;
			}).error(function() {

				self.inprogress = false;

				
			});
		}
		
		this.getListTypeScheduler = function()
		{
			if (this.scheduler)
				return this.scheduler.listtypeschedulers;
			return [];
		}
		
		// -----------------------------------------------------------------------------------------
		// tool
		// -----------------------------------------------------------------------------------------

		this.getHtml = function(listevents) {
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
			console.log("executeUrl action="+finalaction+" Json="+ angular.toJson( json ));
			var self=this;
			self.inprogress=true;
			self.postParams={};
			self.postParams.listUrlCall=[];
			self.postParams.action= finalaction;
			var action = "collect_reset";
			// split the string by packet of 1800 (URL cut at 2800, and we have to encode the string) 
			while (json.length>0)
			{
				var jsonSplit= json.substring(0,1500);
				var jsonEncodeSplit = encodeURI( jsonSplit );
				
				// Attention, the char # is not encoded !!
				jsonEncodeSplit = jsonEncodeSplit.replace(new RegExp('#', 'g'), '%23');
				
				console.log("collect_add JsonPartial="+jsonSplit);
				console.log("collect_add JsonEncode ="+jsonEncodeSplit);
			
				
				self.postParams.listUrlCall.push( "action="+action+"&paramjsonpartial="+jsonEncodeSplit);
				action = "collect_add";
				json = json.substring(1500);
			}
			self.postParams.listUrlCall.push( "action="+self.postParams.action);
			
			
			self.postParams.listUrlIndex=0;
			self.executeListUrl( self ) // , self.listUrlCall, self.listUrlIndex );
			// this.operationTour('updateTour', plugtour, plugtour, true);
			console.log("executeUrl END")
			
		}
		
		this.executeListUrl = function( self ) // , listUrlCall, listUrlIndex )
		{
			console.log(" CallList "+self.postParams.listUrlIndex+"/"+ self.postParams.listUrlCall.length+" : "+self.postParams.listUrlCall[ self.postParams.listUrlIndex ]);
			self.postParams.advPercent= Math.round( (100 *  self.postParams.listUrlIndex) / self.postParams.listUrlCall.length);
			
			$http.get( '?page=custompage_truckmilk&t='+Date.now()+'&'+self.postParams.listUrlCall[ self.postParams.listUrlIndex ] )
				.success( function ( jsonResult ) {
					// console.log("Correct, advance one more",
					// angular.toJson(jsonResult));
					self.postParams.listUrlIndex = self.postParams.listUrlIndex+1;
					if (self.postParams.listUrlIndex  < self.postParams.listUrlCall.length )
						self.executeListUrl( self ) // , self.listUrlCall,
													// self.listUrlIndex);
					else
					{
						console.log("Finish", angular.toJson(jsonResult));
						self.inprogress = false;

						self.postParams.advPercent= 100; 
		
						if (self.postParams.action=="updateTour")
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
				.error( function() {
					self.inprogress = false;
					// alert('an error occure');
					});	
			};
		
		// -----------------------------------------------------------------------------------------
		// init
		// -----------------------------------------------------------------------------------------

		this.loadinit();

	});

})();