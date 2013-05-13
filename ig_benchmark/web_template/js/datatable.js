var current_url_datatable_type = null; 
var current_url_datatable_id = null;
var current_dataset_name = null;
var current_column = null;

function set_column_type()
{
    if(current_url_datatable_type == "suite")
    {
        current_column = suite_column_description;
    }
    else if(current_url_datatable_type == "case")
    {
        current_column = case_column_description;
    }
    else if(current_url_datatable_type == "case_data")
    {
        current_column = case_data_column_description;
    }
}

function set_current_dataset_name()
{
    current_dataset_name = "data/" + current_url_datatable_type + "." + current_url_datatable_id + ".json";
}


function datatable_initialize(url)
{
    var i;
    for(i=1;i<url.length;i++)
    {
        var query = url[i].split("=");
        if(query[0] == "type")
        {
            current_url_datatable_type = query[1]; 
        }
        else if(query[0] == "id")
        {
            current_url_datatable_id = query[1];
        }
    }
    datatable_populate_using_name("suite","1");
}

var case_plot_id = "";

jQuery.fn.dataTableExt.oSort['string-case-asc']  = function(x,y) {
    return ((x < y) ? -1 : ((x > y) ?  1 : 0));
};

jQuery.fn.dataTableExt.oSort['string-case-desc'] = function(x,y) {
    return ((x < y) ?  1 : ((x > y) ? -1 : 0));
};

function read_case_description(id)
{
    var case_description_name = "data/case.description." + id + ".json";
    $.getJSON(case_description_name,function(data)
              {
                  if(data != null)
                  {
                      html_text = "";
                      for(i=0;i<data.dvar.length;i++)
                      {
                          if(data.ivar.length > 0)
                              html_text += "<tr><td><a href=javascript:open_plot_window(\""+data.dvar[i].name+"\");>plot "+data.dvar[i].name+"</a></td></tr>";
                          else
                          {
                              var data_file = "data/case." + data.dvar[i].name + "." + id + ".json";
                              html_text += "<tr><td><a href=javascript:open_simple_plot_window(\""+data.dvar[i].name+"\",\""+data_file+"\");>plot "+data.dvar[i].name+"</a></td></tr>";
                          }
                      }
                      $('#plot_table_link_id').html(html_text);
                  }
              }
              );
}

function datatable_populate_using_name(type,id)
{
    current_url_datatable_type = type;
    current_url_datatable_id = id;
    set_current_dataset_name();
    $('#dynamic').children().remove();
    $('#dynamic').html( '<table cellpadding="0" cellspacing="0" border="0" class="display" id="datatable_id"></table>' );
    $.getJSON(current_dataset_name,function(data)
              {
                  document.getElementById('header_id').innerHTML = data.name;
                  document.getElementById('description_id').innerHTML = data.description;
                  $('#plot_table_link_id').children().remove();
                  if(type == "case")
                  {
                      read_case_description(id);
                      case_plot_id = id;
                  }
                                    
                  var oTable = $('#datatable_id').dataTable( {
                          "aaData": data.data,
                          "aoColumns": data.column_description,
                          "aLengthMenu": [
                              [-1,10,50,75,100],
                              ["All",10,50,75,100]
                              ],
                          "iDisplayLength": -1,
                          //"sScrollY": "200px",
                          //"bPaginate": false,
                          "bScrollAutoCss": false,
                          } );
                  //oTable.fnAdjustColumnSizing(true);
              }
              );
}

/*
function open_time_plot_window()
{
    window.open ("plot.html?id="+case_plot_id+"?type=time","Time Plot","menubar=0,resizable=1,width=1200,height=700");
}

function open_memory_plot_window()
{
    window.open ("plot.html?id="+case_plot_id+"?type=memory","Memory Plot","menubar=0,resizable=1,width=1000,height=700");
}
*/

var plot_counter = 1;
function open_plot_window(type)
{
    window.open ("plot.html?id="+case_plot_id+"?type="+type,"Plot"+type+plot_counter,"menubar=0,resizable=1,width=1200,height=700");
    plot_counter += 1;
}

function open_simple_plot_window(type,data_file)
{
    window.open ("simple_plot.html?id="+case_plot_id+"?type="+type+"?data="+data_file,"Plot"+type+plot_counter,"menubar=0,resizable=1,width=800,height=700");
    plot_counter += 1;
}
