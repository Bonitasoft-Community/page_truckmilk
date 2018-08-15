'use strict';
/**
 * 
 */

(function() {

	var appCommand = angular.module('truckmilk', [ 'googlechart', 'ui.bootstrap' ]);

	// --------------------------------------------------------------------------
	//
	// Controler Ping
	//
	// --------------------------------------------------------------------------

	// Ping the server
	appCommand.controller('TruckMilkControler', function($http, $scope, $sce, $timeout) {

		this.showhistory = function(show) {
			this.isshowhistory = show;
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
		
		this.scheduler={ 'enable': false};
		
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
			
			$http.get('?page=custompage_truckmilk&action=init&timestamp='+Date.now()).success(function(jsonResult) {
				console.log("history", jsonResult);
				self.inprogress = false;

				self.listplugtour 	= jsonResult.listplugtour;
				self.listplugin 	= jsonResult.listplugin;
				self.scheduler 		= jsonResult.scheduler;
				self.listevents 	= jsonResult.listevents;
				console.log("TruckMilk RECEPTION TOUR : init them");
				// prepare defaut value
				for ( var i in self.listplugtour) {
					var plugtourindex = self.listplugtour[i];
					self.preparePlugTourParameter(plugtourindex);
				}
			}).error(function() {

				self.inprogress = false;

				self.listplugtour = [];
				self.listplugin = [];

			});

		}

		this.completeRefresh=false;
		// Completerefresh : refresh the complete list, else refresh only some attributes
		this.refresh = function( completeRefresh ) {
			console.log("truckmilk:refresh ["+completeRefresh+"]");
			var self = this;
			self.inprogress = true;
			self.listevents='';
			self.completeRefresh = completeRefresh;
			
			// rearm the timer
			$scope.timer = $timeout(function() { self.refresh( false ) }, 120000);

						
			$http.get('?page=custompage_truckmilk&action=refresh&timestamp='+Date.now()).success(function(jsonResult) {
				// console.log("history", jsonResult);
				self.inprogress = false;

				if (self.completeRefresh)
				{
					self.listplugtour 	= jsonResult.listplugtour;
					// prepare defaut value
					for ( var i in self.listplugtour) {
						var plugtourindex = self.listplugtour[i];
						self.preparePlugTourParameter(plugtourindex);
					}
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
				self.scheduler 		= jsonResult.scheduler;

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
			var param = {
				'name' :  plugtour.name,
				'id'   :  plugtour.id
			};
			this.operationTour('removeTour', param, null, false);
		}

		this.stopTour = function( plugtour) {
			this.operationTour('stopTour', plugtour, plugtour, false);
		}
		this.startTour = function(plugtour) {
			this.operationTour('startTour', plugtour, plugtour, false);
		}
		this.updatePlugtour= function(plugtour) {
			var paramStart = plugtour;
			this.operationTour('updateTour', plugtour, plugtour, true);
		}
		
		this.immediateTour = function(plugtour) {
			var paramStart = plugtour;
			this.operationTour('immediateExecution', plugtour, plugtour, true);		
		}
		this.operationTour = function(action, param, plugtour, refreshParam) {
			var self = this;
			self.inprogress = true;
			self.currentplugtour = plugtour;
			self.refreshParam = refreshParam;
			var json = encodeURIComponent(angular.toJson(param, true));
			self.listevents='';
			
			$http.get('?page=custompage_truckmilk&action=' + action + '&paramjson=' + json+'&timestamp='+Date.now()).success(function(jsonResult) {
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

				if (self.currentplugtour != null)
					self.currentplugtour.listevents = jsonResult.listevents;
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

		this.preparePlugTourParameter = function(plugtour) {
			console.log("PlugTourPreration " + plugtour.name);
			plugtour.newname=plugtour.name;
			plugtour.parametersvalue = {};
			for ( var key in plugtour.parameters) {
				plugtour.parametersvalue[key] = plugtour.parameters[key];
				// console.log("Parameter[" + key + "] value[" + plugtour.parameters[key] + "] ="+angular.toJson( plugtour.parametersvalue,true ));
			}
			// console.log("PlugTourPreration END " + plugtour.name+" plugtour="+angular.toJson( plugtour,true ));

		}
		
		// -----------------------------------------------------------------------------------------
		// manupulate array in parameters
		this.isAnArray = function( value ) {
		     return Array.isArray( value );
	     }
		this.addInArray = function( valueArray)
		{
			// console.log("valueArray="+angular.toJson( valueArray ));
			valueArray.push( "");
			// console.log("add valueArray="+angular.toJson( valueArray ));
		}
		this.removeInArray = function( valueArray, value)
		{
			console.log("remove value "+value);
			var index = valueArray.indexOf(value);
			if (index > -1) {
				valueArray.splice(index, 1);
			}
			console.log("remove value ["+value+"] index ["+index+"] valueArray="+angular.toJson( valueArray ));
		}

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
				return "btn btn-danger btn-xs"
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
			
			$http.get('?page=custompage_truckmilk&action=scheduler&paramjson=' + json+'&timestamp='+Date.now()).success(function(jsonResult) {
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
			
			$http.get('?page=custompage_truckmilk&action=schedulermaintenance&paramjson=' + json+'&timestamp='+Date.now()).success(function(jsonResult) {
				console.log("scheduler", jsonResult);
				self.inprogress = false;

				self.scheduler.status 		= jsonResult.status;
				self.scheduler.schedulerlistevents	= jsonResult.listevents;
			}).error(function() {

				self.inprogress = false;

				
			});
		}
		
		this.getListTypeScheduler = function()
		{
			return this.scheduler.listtypeschedulers;
		}
		
		// -----------------------------------------------------------------------------------------
		// tool
		// -----------------------------------------------------------------------------------------

		this.getListEvents = function(listevents) {
			return $sce.trustAsHtml(listevents);
		}

		// -----------------------------------------------------------------------------------------
		// init
		// -----------------------------------------------------------------------------------------

		this.loadinit();

	});

})();