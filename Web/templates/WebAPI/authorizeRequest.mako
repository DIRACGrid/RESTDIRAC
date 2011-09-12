# -*- coding: utf-8 -*-
<%inherit file="/diracPage4.mako" />

<%def name="head_tags()">

<%
htmlCode = "<form action='grantAccess' method='post'>"
htmlCode += "<input type='hidden' name='ticket' value='%s'/>" % c.ticket
htmlCode += "<input type='hidden' name='request' value='%s'/>" % c.request
htmlCode += "<table class='access'>"
htmlCode += "<tr><td class='header'>Do you grant access %s to access your account in DIRAC?</td>" % c.consName
if c.consImg:
  htmlCode += "<td rowspan=3><img src='%s'/></td>" % c.consImg
htmlCode += "</tr>"
htmlCode += "<tr><td>You are %s with group <b>%s</b><br/> If you want to change group, please do so before granting access.</td></tr>" % ( c.userDN, c.userGroup )
htmlCode += "<tr><td>You're granting access for <input type='text' name='accessTime' value='24' size='2'/> hours<td></tr>"
htmlCode += "<tr class='submit'><td><input type='submit' name='grant' value='Grant'/> <input type='submit' name='grant' value='Deny'/><td></tr>"
htmlCode += "</table></form>"
%>

<style type="text/css">

table.access {
  margin-left:auto;
  margin-right:auto;
  font-size:150%;
  margin:10%;
}


table.access td{
  padding: 1%
}

table.access tr.submit td{
  text-align : center;
}

table.access td.header{
  font-size : 200%;
  spacing : 5%;
}

</style>


<script type="text/javascript">

function initAuthPage(){
  Ext.onReady(function(){
    renderPage();
  });
}

function renderPage()
{
  var formPanel = Ext.create( 'Ext.panel.Panel', {
    frame : false,
    region : 'center',
    html : "${htmlCode}"    
    } );
  renderInMainViewport( [ formPanel ] );
}

</script>
 
</%def>


<%def name='body()'>

<script type="text/javascript">
   initAuthPage();
</script>
  
</%def>
