var webPage = require('webpage');
var page = webPage.create();

var fs = require('fs');
var path = 'ezsearch_fpts.html'

page.open('https://ezsearch.fpts.com.vn/Services/EzData/default2.aspx?s=184', function (status) {
  var content = page.content;
  fs.write(path,content,'w')
  phantom.exit();
});