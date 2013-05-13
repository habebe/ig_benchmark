var number_of_populated_selections = 0;
var dataset = [];
var data_name_list = [];
var title = "";
var data_id = null;
var option_selection_id = [];
var data_type = "time";

function selection_change_callback()
{
    var tag_value = $("#tag_id option:selected").val();
    var template_value = $("#template_id option:selected").val();
    var sim_value = $("#sim_id option:selected").val();
    var simtype_value = $("#simtype_id option:selected").val();

    var time_factor_data_name = "data/time_factor." + tag_value + "." + sim_value + "." + template_value + "." + simtype_value +".json";
    populate_time_factor_selection(time_factor_data_name);
}

function get_current_file_name()
{
    var data_name = "data/case." + data_type + "." + data_id + ".";
    var i;
    for(i=0;i<option_selection_id.length;i++)
    {
        var value = $("#"+option_selection_id[i]+" option:selected").val();
        data_name +=  value + ".";
    }
    data_name += "json";
    return data_name;
}

function populate_selection(selection_object,data_name)
{
    selection_object.children().remove();
    $.getJSON(data_name,function(data)
              {
                  for(var i=0;i < data.length;i++ )
                  {
                      selection_object.append('<option value='+data[i].id+'>'+data[i].name+'</option>');
                  }
                  number_of_populated_selections += 1;
                  if(number_of_populated_selections == 4)
                  {
                      selection_change_callback();
                  }
              });
}

function populate_time_factor_selection(data_name)
{

    $('#time_factor_id').children().remove();
    $.getJSON(data_name,function(data)
              {
                  for(var i=0;i < data.length;i++ )
                  {
                      ($("#time_factor_id")).append('<option value='+data[i].id+'>'+data[i].name+'</option>');
                  }
              }
              );
}

var plot_is_show_points_checked = true;
var plot_is_show_lines_checked = true;
var plot_is_show_legends_checked = true;
function plot_current_data()
{
    $.plot($("#plot_placeholder"), dataset,
           {
             xaxis: {
                 show: true,
                   min:null,
                   max:null
                   },
               points: {
                 show:plot_is_show_points_checked
                   },
               lines: {
                 show:plot_is_show_lines_checked
                   },
               legend:
               {
                 position:"nw",
                   show:plot_is_show_legends_checked
                   }
           }
           );
}

function plot_data_using_name(name)
{
    $.getJSON(name,function(data)
              {
                  dataset.push(data);
                  //alert(data.xaxis);
                  document.getElementById('x_axis_label_id').innerHTML = data.xaxis;
                  plot_current_data();
                  set_current_link();
              }
              );
}

function plot_data()
{
    var data_name = get_current_file_name();
    plot_data_using_name(data_name);
    data_name_list.push(data_name);
}

function plot_data_list()
{
    var i;
    for(i=0;i<data_name_list.length;i++)
    {
        plot_data_using_name(data_name_list[i]);
    }
}

function plot_clear_function()
{
    dataset = [];
    data_name_list = [];
    plot_current_data();
    set_current_link();
}

function plot_show_points_function()
{
    plot_is_show_points_checked = !plot_is_show_points_checked;
    plot_current_data();
}

function plot_show_lines_function()
{
    plot_is_show_lines_checked = !plot_is_show_lines_checked;
    plot_current_data();
}

function plot_show_legends_function()
{
    plot_is_show_legends_checked = !plot_is_show_legends_checked;
    plot_current_data();
}

function initialize_table_selection()
{
    var case_description_name = "data/case.description." + data_id + ".json";
    $.getJSON(case_description_name,function(data)
              {
                  

                  document.getElementById('title_link_id').innerHTML = data.name;
                  document.getElementById('page_title_id').innerHTML = data.name;
                  document.getElementById('description_id').innerHTML = "[" + data_type + "] " + data.description;
                  var selection_element = document.getElementById('ivar_table_selection_id');
                  var html_text = "";
                  var option_html_text = "";
                  var _data_ = null;
                  var i;
                  var j;
                  option_selection_id = [];
                  for(i=0;i<data.ivar.length;i++)
                  {
                      var selection_name = "option_selection_" + i;
                      var selection_id   = selection_name + "_id";
                      _data_ = data.ivar[i];
                      
                      html_text += "<tr><td valign=\"top\"><label>"+_data_.name+"</label></td></tr>";
                      html_text += "<tr><td valign=\"top\"><select name=\""+selection_name+"\" id=\""+selection_id+"\">";
                      option_selection_id.push(selection_id);
                      option_html_text = "";
                      for(j=0;j<_data_.data.length;j++)
                      {
                          option_html_text += '<option value='+_data_.data[j].id+'>'+_data_.data[j].name+'</option>';
                      }
                      html_text += option_html_text;
                      html_text += "</select></td></tr>";
                  }
                  selection_element.innerHTML = html_text;
                  
              }
              );
}

function initialize_data(url)
{
    if(url.length > 1)
    {
        var file_names = [];
        var i;
        for(i=1;i<url.length;i++)
        {
            var query = url[i].split("=");
            if(query[0] == "data")
            {
                data_name_list.push(query[1]);
            }
            else if(query[0] == "id")
            {
                data_id = query[1];
            }
            else if(query[0] == "type")
            {
                data_type = query[1];
            }
            
        }
    }
    initialize_table_selection();
    
}

function set_current_link()
{
    var url = document.URL.split("?");
    var base = url[0];
    var i;
    base += "?id=" + data_id;
    base += "?type=" + data_type;
    for(i=0;i<data_name_list.length;i++)
    {
        base += "?data="+data_name_list[i];
    }
    var reference = document.getElementById('title_link_id'); 
    reference.href = base;
}

function setup_plot_toolbar()
{
    $(function()
      {
          $( "#plot_show_points_id" ).button().click(plot_show_points_function);
          $( "#plot_show_lines_id" ).button().click(plot_show_lines_function);
          $( "#plot_show_legends_id" ).button().click(plot_show_legends_function);
          $( "#plot_clear_id" ).button().click(plot_clear_function);
          $( "#plot_button_data_id").button().click(plot_data);
      }
      );
}

