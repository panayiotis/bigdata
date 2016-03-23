var wordcloud;

wordcloud = function(element, words) {
  var draw, fill, height, j, layout, len, max, min, width, word;
  max = 0;
  min = 10000;
  for (j = 0, len = words.length; j < len; j++) {
    word = words[j];
    if (word.size > max) {
      max = word.size;
    }
    if (word.size < min) {
      min = word.size;
    }
  }
  console.log(element + " wordcloud generation " + min + " - " + max);
  fill = d3.scale.category20();
  height = d3.select('#tabs').node().getBoundingClientRect().height - 40;
  width = d3.select(element).node().getBoundingClientRect().width - 20;
  draw = function(words) {
    return d3.select(element).append('svg').attr('width', layout.size()[0]).attr('height', layout.size()[1]).append('g').attr('transform', 'translate(' + layout.size()[0] / 2 + ',' + layout.size()[1] / 2 + ')').selectAll('text').data(words).enter().append('text').style('font-size', function(d) {
      return d.size + 'px';
    }).style('font-family', 'Impact').style('fill', function(d, i) {
      return fill(i);
    }).attr('text-anchor', 'middle').attr('transform', function(d) {
      return 'translate(' + [d.x, d.y] + ')rotate(' + d.rotate + ')';
    }).text(function(d) {
      return d.text;
    });
  };
  layout = d3.layout.cloud().size([width, height]).words(words).padding(5).rotate(function() {
    return ~~(Math.random() * 2) * 90;
  }).font('Impact').fontSize(function(d) {
    return ((d.size - min) / (max - min)) * 90 + 10;
  }).on('end', draw);
  return layout.start();
};

$(function() {
  var cloud, el, first, j, len, ref, results, words;
  d3.select('#tabs').append('ul').selectAll('li').data(clouds).enter().append('li').append('a').attr("id", function(d) {
    return d.category.replace(/\s/g, '') + "-label";
  }).attr("href", function(d) {
    return "#" + d.category.replace(/\s/g, '');
  }).text(function(d) {
    return d.category;
  });
  d3.select('#tabs').append('div').attr('class', 'container').selectAll('div').data(clouds).enter().append('div').attr("id", function(d) {
    return d.category.replace(/\s/g, '');
  }).append('div').attr('class', 'loader');
  $('#tabs').tabs().addClass('tabs ui-tabs-vertical ui-helper-clearfix');
  $('#tabs li').removeClass('ui-corner-top').addClass('ui-corner-left');
  first = true;
  ref = window.clouds;
  results = [];
  for (j = 0, len = ref.length; j < len; j++) {
    cloud = ref[j];
    el = "#" + cloud.category.replace(/\s/g, '');
    words = cloud.words;
    if (first) {
      wordcloud(el, words);
      $(el).find('.loader').remove();
      results.push(first = false);
    } else {
      results.push((function(el, words) {
        return $(el + "-label").one('click', function() {
          return (function(el, words) {
            return setTimeout((function() {
              wordcloud(el, words);
              return $(el).find('.loader').remove();
            }), 100);
          })(el, words);
        });
      })(el, words));
    }
  }
  d3.select('#tabs .container').style("height",d3.select('#tabs').node().getBoundingClientRect().height+"px")
});
