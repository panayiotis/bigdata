(function() {
  window.createrocs = function(element, inputrocs) {
    var j, len, results, roc, rocs;
    rocs = (function() {
      var j, len, results;
      results = [];
      for (j = 0, len = inputrocs.length; j < len; j++) {
        roc = inputrocs[j];
        roc.uid = "roc-" + Math.random().toString(36).substr(2, 6);
        results.push(roc);
      }
      return results;
    })();
    console.log(rocs);
    d3.select(element).attr('class', "tabs").append('ul').selectAll('li').data(rocs).enter().append('li').append('a').attr("id", function(d) {
      return "label-" + d.uid;
    }).attr("href", function(d) {
      return "#" + d.uid;
    }).text(function(d) {
      return d.desc.split(" ")[0];
    });
    d3.select(element).append('div').attr("class","container").selectAll('div').data(rocs).enter().append('div').attr("id", function(d) {
      return d.uid;
    }).append('div').attr('class', 'loader');
    $(element).tabs().addClass('ui-tabs-vertical ui-helper-clearfix');
    $(element + ' li').removeClass('ui-corner-top').addClass('ui-corner-left');
    results = [];
    for (j = 0, len = rocs.length; j < len; j++) {
      roc = rocs[j];
      console.log(roc.desc);
      createroc("#" + roc.uid, roc.roc);
      results.push($("#" + roc.uid).find('.loader').remove());
    }
    d3.select(element +' .container').style("height",d3.select('#tabs').node().getBoundingClientRect().height+"px")
  };

  window.createroc = function(element, rocs, height, width) {
    var color, legend, line, margin, roc, svg, x, xAxis, y, yAxis;
    if (height == null) {
      height = 500;
    }
    if (width == null) {
      width = 700;
    }
    margin = {
      top: 20,
      right: 130,
      bottom: 30,
      left: 40
    };
    console.log(width);
    x = d3.scale.linear().range([0, width]);
    y = d3.scale.linear().range([height, 0]);
    color = d3.scale.category20();
    xAxis = d3.svg.axis().scale(x).orient('bottom');
    yAxis = d3.svg.axis().scale(y).orient('left');
    line = d3.svg.line().interpolate('basis').x(function(d) {
      return x(d[0]);
    }).y(function(d) {
      return y(d[1]);
    });
    svg = d3.select(element).append('svg').attr('width', width + margin.left + margin.right).attr('height', height + margin.top + margin.bottom).append('g').attr('transform', 'translate(' + margin.left + ',' + margin.top + ')');
    x.domain([0, 1]);
    y.domain([0, 1]);
    svg.append('g').attr('class', 'x axis').attr('transform', 'translate(0,' + height + ')').call(xAxis).append('text').attr('y', -10).attr('x', width).style('text-anchor', 'end').text('False positive rate');
    svg.append('g').attr('class', 'y axis').call(yAxis).append('text').attr('transform', 'rotate(-90)').attr('y', 6).attr('dy', '.71em').style('text-anchor', 'end').text('True positive rate');
    roc = svg.selectAll('.roc').data(rocs).enter().append('g').attr('class', 'roc');
    roc.append('path').attr('class', 'line').attr('d', function(d) {
      return line(d.values);
    }).style('stroke', function(d) {
      return color(d.name);
    });
    legend = svg.selectAll('.legend').data(color.domain()).enter().append('g').attr('class', 'legend').attr('transform', function(d, i) {
      return 'translate(0,' + i * 20 + ')';
    });
    legend.append('rect').attr('x', width + 10).attr('width', 18).attr('height', 18).style('fill', color);
    return legend.append('text').attr('x', width + 30).attr('y', 9).attr('dy', '.35em').style('text-anchor', 'start').text(function(d) {
      return d;
    });
  };

}).call(this);

