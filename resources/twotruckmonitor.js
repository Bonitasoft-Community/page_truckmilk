'use strict';
/**
 *
 */

(function() {


var appCommand = angular.module('towtruck', ['googlechart', 'ui.bootstrap']);






// --------------------------------------------------------------------------
//
// Controler Ping
//
// --------------------------------------------------------------------------

// Ping the server
appCommand.controller('TowTrucControler',
	function ( $http, $scope ) {
	this.isshowhistory=false;
	this.pingdate='';
	this.pinginfo='';
	$('#collectwait').hide();

	
	this.showhistory = function( show )
	{
	   this.isshowhistory = show;
	}

	this.createmissingtimers = function( typeCreation)
	{

		$('#createbtn').hide();
		$('#collectwait').show();
		
		var self=this;

		$http.get( '?page=custompage_towtruck&action=createmissingtimers&typecreation='+typeCreation )
				.success( function ( jsonResult ) {
						console.log("history",jsonResult);
						self.listtimers 		= jsonResult.listtimers;
						self.missingtimerstatus	= jsonResult.missingtimerstatus;
						self.timererror 		= jsonResult.timererror;
						$('#createbtn').show();
						$('#collectwait').hide();
				})
				.error( function() {
					
						self.timerstatus 		= jsonResult.timerstatus;
						self.timererror 		= jsonResult.timererror;
						
						$('#createbtn').show();
						$('#collectwait').hide();
					});
				
	}; 
	
	this.getmissingtimer = function()
	{
		$('#collectbtn').hide();
		$('#collectwait').show();
		this.pinginfo="Hello";
		
		var self=this;

		$http.get( '?page=custompage_towtruck&action=getmissingtimer' )
				.success( function ( jsonResult ) {
						console.log("history",jsonResult);
						self.listtimers 		= jsonResult.listtimers;
						self.timerstatus 		= jsonResult.timerstatus;
						self.timererror 		= jsonResult.timererror;
								
						$('#collectbtn').show();
						$('#collectwait').hide();
				})
				.error( function() {
					alert('an error occured');
						self.timererror 		= jsonResult.timererror;
						$('#collectbtn').show();
						$('#collectwait').hide();
					});
				
	}; // end getmissingtimer

	
	this.deletetimers = function()
	{
		$('#collectbtn').hide();
		$('#collectwait').show();
		
		var self=this;

		$http.get( '?page=custompage_towtruck&action=deletetimers' )
				.success( function ( jsonResult ) {
						console.log("history",jsonResult);
						self.listtimers 		= jsonResult.listtimers;
						self.timerstatus 		= jsonResult.timerstatus;
						self.timererror 		= jsonResult.timererror;
								
						$('#collectbtn').show();
						$('#collectwait').hide();
				})
				.error( function() {
					alert('an error occured');
						self.timerstatus 		= jsonResult.timerstatus;
						self.timererror 		= jsonResult.timererror;
						$('#collectbtn').show();
						$('#collectwait').hide();
					});
				
	}; // end deletetimer


});



})();