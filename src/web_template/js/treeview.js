
var treeview_html_string = "";

function treeview_populate(treeview_object,data)
{
    var i;
  
    for(i=0;i<data.length;i++)
    {
        var item_data = data[i];
        treeview_html_string += '<li><a href="'+item_data.ref+'">';

        console.log(item_data.name + " ref:" +item_data.ref);
        
        if(item_data.strong == 1)
            treeview_html_string += "<strong>";
        treeview_html_string += item_data.name;
        if(item_data.strong == 1)
            treeview_html_string += "</strong>";

        treeview_html_string += "</a>";
        
        if (item_data.data.length > 0)
        {
            treeview_html_string += "<ul>";
            treeview_populate(treeview_object,item_data.data);
            treeview_html_string += "</ul>";
        }
        treeview_html_string += "</li>";
    }
}

function treeview_initialize(treeview_object,data_name)
{
    treeview_object.children().remove();
    $.getJSON(data_name,
              function(data)
              {
                  treeview_html_string = "";
                  treeview_populate(treeview_object,data);
                  treeview_object.append(treeview_html_string);
                  treeview_object.treeview({
                        collapsed: false,
                          animated: "fast",
                          control:"#sidetreecontrol",
                          persist: "location"
                          });
              }
              );
}

