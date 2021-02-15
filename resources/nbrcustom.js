/**
 * validates and manupulates custom height and weight data entry
 * to calculate bmi value
 */
CRM.$(document).ready(function($) {

  //console.log('nbrcustomjs call')

  function keyNumber(evt, element) {                                  // allow numeric input only
    var charCode = (evt.which) ? evt.which : evt.keyCode
    if (                                                              // allowed chars:
      ((charCode != 45) || $(element).val().indexOf('-') != -1) &&    //  - minus OK but only one
      (charCode != 46 || $(element).val().indexOf('.') != -1) &&      //  . dot OK but only one
      (charCode < 48 || charCode > 57) &&                             //  numeric char
      (charCode!=8)) {                                                //  BS
      return false;
    }
    return true;
  }

  // check entered value in range 0 - max
  function in_range(val, max) {
    if (parseFloat(val)<0||parseFloat(val)>max) {return false;} else {return true;}
  }

  // set value of bmi field
  function set_bmi(ht,wt) {
    var bmi_val = parseFloat(wt)/(parseFloat(ht)*parseFloat(ht));
    bmi_obj.val(bmi_val.toFixed(1));

  }
  // get ht, wt, bmi objects
  ht_obj=$("[data-crm-custom='nihr_volunteer_general_observations:nvgo_height_m']");
  wt_obj=$("[data-crm-custom='nihr_volunteer_general_observations:nvgo_weight_kg']");
  bmi_obj=$("[data-crm-custom='nihr_volunteer_general_observations:nvgo_bmi']");
  bmi_obj.attr('readonly',true);

  // prevent non-numeric input
  ht_obj.addClass('numberinput')
  wt_obj.addClass('numberinput')
  $(".numberinput").keypress(function (event) {return keyNumber(event, this);});

  // event handlers - calculate bmi if valid ht/wt entered, otherwise set invalid value and bmi to zero
  $(ht_obj).blur(function() {
    ht = $(this).val();
    wt = wt_obj.val();
    (in_range(ht, 3))?set_bmi(ht,wt):ht_obj.val('0');
  });
  $(wt_obj).blur(function() {
    wt = $(this).val();
    ht = ht_obj.val();
    (in_range(wt, 300))?set_bmi(ht,wt):wt_obj.val('0');
  });

})
