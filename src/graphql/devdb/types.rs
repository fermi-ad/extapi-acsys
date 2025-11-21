use async_graphql::{ComplexObject, Interface, SimpleObject, Union};

// Pull in global types.

use crate::graphql::types as global;

#[allow(clippy::duplicated_attributes)] // Needed to stop flagging false positive in `ty` attributes
#[doc = "Common set of attributes for reading and setting properties."]
#[derive(Interface)]
#[graphql(
    field(name = "primary_units", ty = "&Option<String>"),
    field(name = "common_units", ty = "&Option<String>"),
    field(name = "min_val", ty = "&f64"),
    field(name = "max_val", ty = "&f64"),
    field(name = "primary_index", ty = "&u32"),
    field(name = "common_index", ty = "&u32"),
    field(name = "coeff", ty = "&Vec<f64>"),
    field(name = "is_step_motor", ty = "&bool"),
    field(name = "is_destructive_read", ty = "&bool"),
    field(name = "is_fe_scaling", ty = "&bool"),
    field(name = "is_contr_setting", ty = "&bool")
)]
pub enum DeviceProperty {
    ReadingProp(ReadingProp),
    SettingProp(SettingProp),
}

#[doc = "Holds data associated with the reading property of a device."]
#[derive(SimpleObject)]
pub struct ReadingProp {
    #[doc = "Specifies the engineering units for the primary transform of \
	     the device. This field might be `null`, if there aren't units \
	     for this transform."]
    pub primary_units: Option<String>,

    #[doc = "Specifies the engineering units for the common transform of \
	     the device. This field might be `null`, if there aren't units \
	     for this transform."]
    pub common_units: Option<String>,

    #[doc = "The maximum value this device will read and allow to be set. \
	     This field is a recommendation for applications to follow. \
	     The actual hardware driver will enforce the limits."]
    pub min_val: f64,

    #[doc = "The minimum value this device will read and allow to be set. \
	     This field is a recommendation for applications to follow. The \
	     actual hardware driver will enforce the limits."]
    pub max_val: f64,

    #[doc = "The index of the primary scaling transform."]
    pub primary_index: u32,

    #[doc = "The index of the common scaling transform."]
    pub common_index: u32,

    #[doc = "The coefficients to be used with the common scaling transform. \
	     There will be 0 - 10 coefficients, depending on the transform. \
	     The transform documentation refers to the constants as \"c1\" \
	     through \"c10\". These correspond to the indices 0 through 9, \
	     respectively."]
    pub coeff: Vec<f64>,

    #[doc = "Indicates whether the property is associated with a stepper \
	     motor."]
    pub is_step_motor: bool,

    #[doc = "Indicates whether reading the property results in a destructive \
	     read."]
    pub is_destructive_read: bool,

    #[doc = "Indicates that the front-end does the scaling for this property."]
    pub is_fe_scaling: bool,

    #[doc = "UNKNOWN"]
    pub is_contr_setting: bool,
}

#[doc = "Holds information about \"knobbing\" a device's setting value."]
#[derive(SimpleObject)]
pub struct KnobInfo {
    #[doc = "The minimum value of the device. When knobbing, the setting \
	     shouldn't go lower than this value."]
    pub min_val: f64,

    #[doc = "The maximum value of the device. When knobbing, the setting \
	     shouldn't go higher than this value."]
    pub max_val: f64,

    #[doc = "The recommended step size when sending a stream of settings."]
    pub step: f64,
}

impl KnobInfo {
    pub fn new(min_val: f64, max_val: f64, step: f64) -> Self {
        KnobInfo {
            min_val: min_val.min(max_val),
            max_val: min_val.max(max_val),
            step,
        }
    }
}

#[doc = "Holds data associated with the setting property of a device."]
#[derive(SimpleObject)]
#[graphql(complex)]
pub struct SettingProp {
    #[doc = "Specifies the engineering units for the primary transform of \
	     the device. This field might be `null`, if there aren't units \
	     for this transform."]
    pub primary_units: Option<String>,

    #[doc = "Specifies the engineering units for the common transform of the \
	     device. This field might be `null`, if there aren't units for \
	     this transform."]
    pub common_units: Option<String>,

    #[doc = "The maximum value this device will read and allow to be set. \
	     This field is a recommendation for applications to follow. The \
	     actual hardware driver will enforce the limits."]
    pub min_val: f64,

    #[doc = "The minimum value this device will read and allow to be set. \
	     This field is a recommendation for applications to follow. The \
	     actual hardware driver will enforce the limits."]
    pub max_val: f64,

    #[doc = "The index of the primary scaling transform."]
    pub primary_index: u32,

    #[doc = "The index of the common scaling transform."]
    pub common_index: u32,

    #[doc = "The coefficients to be used with the common scaling transform. \
	     There will be 0 - 10 coefficients, depending on the transform. \
	     The transform documentation refers to the constants as \"c1\" \
	     through \"c10\". These correspond to the indices 0 through 9, \
	     respectively."]
    pub coeff: Vec<f64>,

    #[doc = "Indicates whether the property is associated with a stepper \
	     motor."]
    pub is_step_motor: bool,

    #[doc = "Indicates whether reading the property results in a destructive \
	     read."]
    pub is_destructive_read: bool,

    #[doc = "Indicates that the front-end does the scaling for this property."]
    pub is_fe_scaling: bool,

    #[doc = "UNKNOWN"]
    pub is_contr_setting: bool,

    #[doc = "Indicates that this device can be \"knobbed\" (i.e. it accepts \
	     a rapid stream of settings.)"]
    #[graphql(skip)]
    pub is_knobbable: bool,
}

#[ComplexObject]
impl SettingProp {
    #[doc = "If the device has associated \"knobbing\" information, this \
	     field will specify the configuration."]
    async fn knob_info(&self) -> Option<KnobInfo> {
        if self.is_knobbable {
            if self.common_index == 40 && self.coeff.len() >= 6 {
                Some(KnobInfo::new(self.coeff[3], self.coeff[4], self.coeff[5]))
            } else {
                let inc = match self.primary_index {
                    16 | 22 | 24 | 84 => 0.005,
                    _ => 16.0,
                };

                Some(KnobInfo::new(self.min_val, self.max_val, inc))
            }
        } else {
            None
        }
    }
}

#[doc = "Represents a legacy form to describe a basic status bit.

The BASIC STATUS property of a device traditionally modeled a power supply's \
set of status bits (on/off, ready/tripped, etc.) This structure models the \
data associated with each of these statuses and allows them to be renamed."]
#[derive(SimpleObject)]
pub struct DigStatusEntry {
    #[doc = "This value is logically ANDed with the active, raw status to \
	     filter the bit that aren't related to the current status."]
    pub mask_val: u32,

    #[doc = "This is the value that the masked status needs to be in order \
	     to consider it in a good state."]
    pub match_val: u32,

    #[doc = "If this field is true, then the raw status is complemented \
	     before masking."]
    pub invert: bool,

    #[doc = "A short name for this status."]
    pub short_name: String,

    #[doc = "A longer version of the name of this status."]
    pub long_name: String,

    #[doc = "A string representing the value when it's in a good state."]
    pub true_str: String,

    #[doc = "The color to use when the status is in a good state."]
    pub true_color: u32,

    #[doc = "A character to display that represents a good state."]
    pub true_char: String,

    #[doc = "A string representing the value when it's in a bad state."]
    pub false_str: String,

    #[doc = "The color to use when the status is in a bad state."]
    pub false_color: u32,

    #[doc = "A character to display that represents a bad state."]
    pub false_char: String,
}

#[doc = "Represents a more modern way to define the bits in the basic status."]
#[derive(SimpleObject)]
pub struct DigExtStatusEntry {
    #[doc = "Indicates with which bit in the status this entry corresponds. \
	     The LSB is 0."]
    pub bit_no: u32,

    #[doc = "The color to use when this bit is `false`."]
    pub color0: u32,

    #[doc = "The descriptive name when this bit is `false`."]
    pub name0: String,

    #[doc = "The color to use when this bit is `true`."]
    pub color1: u32,

    #[doc = "The descriptive name when this bit is `false`."]
    pub name1: String,

    #[doc = "The description of this bit's purpose."]
    pub description: String,
}

#[doc = "The configuration of the device's basic status property.

This structure contains both the legacy and modern forms of configurations \
used to describe a device's basic status property."]
#[derive(SimpleObject)]
pub struct DigStatus {
    #[doc = "Holds the legacy, \"power supply\" configuration."]
    pub entries: Vec<DigStatusEntry>,

    #[doc = "Hold the modern, bit definitions."]
    pub ext_entries: Vec<DigExtStatusEntry>,
}

#[doc = "Describes one digital control command used by a device."]
#[derive(SimpleObject)]
pub struct DigControlEntry {
    #[doc = "The actual integer value to send to the device in order to \
	     perform the command."]
    pub value: i32,

    #[doc = "The name of the command and can be used by applications to \
	     create a descriptive menu."]
    pub short_name: String,

    #[doc = "A more descriptive name of the command."]
    pub long_name: String,
}

#[doc = "Describes the digital control commands for a device."]
#[derive(SimpleObject)]
pub struct DigControl {
    pub entries: Vec<DigControlEntry>,
}

#[doc = "A structure containing device information."]
#[derive(SimpleObject)]
pub struct DeviceInfo {
    #[doc = "A text field that summarizes the device's purpose."]
    pub description: String,

    #[doc = "Holds informations related to the reading property. If the \
	     device doesn't have a reading property, this field will be \
	     `null`."]
    pub reading: Option<ReadingProp>,

    #[doc = "Holds informations related to the setting property. If the \
	     device doesn't have a setting property, this field will be \
	     `null`."]
    pub setting: Option<SettingProp>,

    pub dig_control: Option<DigControl>,
    pub dig_status: Option<DigStatus>,
}

#[allow(clippy::large_enum_variant)]
#[doc = "The result of the device info query. It can return device \
	 information or an error message describing why information \
	 wasn't returned."]
#[derive(Union)]
pub enum DeviceInfoResult {
    DeviceInfo(DeviceInfo),
    ErrorReply(global::ErrorReply),
}

#[doc = "The reply to the deviceInfo query."]
#[derive(SimpleObject)]
pub struct DeviceInfoReply {
    pub result: Vec<DeviceInfoResult>,
}
