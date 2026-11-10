//! Tests for the acsys module

use super::*;
use crate::g_rpc::proto::{
    google::protobuf::Timestamp, services::clock_event::EventInfo,
};

// -----------------------------------------------------------------------
// strip_event

#[test]
fn test_removing_event() {
    assert_eq!(strip_event("abc"), "abc");
    assert_eq!(strip_event("abc@e,23"), "abc");
    assert_eq!(strip_event("abc @e,23"), "abc");

    assert_eq!(strip_event(""), "");
    assert_eq!(strip_event("@"), "");
    assert_eq!(strip_event(" @"), "");
}

// Multiple `@` characters — only the first one is the delimiter.
#[test]
fn test_removing_event_multiple_at_signs() {
    assert_eq!(strip_event("abc@e,23@extra"), "abc");
}

// Trailing whitespace before `@` is trimmed.
#[test]
fn test_removing_event_trailing_whitespace_trimmed() {
    assert_eq!(strip_event("M:OUTTMP  @p,1000000u"), "M:OUTTMP");
    assert_eq!(strip_event("M:OUTTMP\t@p,1000000u"), "M:OUTTMP");
}

// A DRF with a source specifier but no event — strip_event leaves the
// source part intact.
#[test]
fn test_removing_event_leaves_source_intact() {
    assert_eq!(strip_event("abc<-LOGGER"), "abc<-LOGGER");
}

// -----------------------------------------------------------------------
// strip_source

#[test]
fn test_removing_source() {
    assert_eq!(strip_source("abc"), "abc");
    assert_eq!(strip_source("abc@e,23"), "abc@e,23");
    assert_eq!(strip_source("abc<-JUNK"), "abc");
    assert_eq!(strip_source("abc <-JUNK"), "abc");
    assert_eq!(strip_source("abc@e,23<-JUNK"), "abc@e,23");
    assert_eq!(strip_source("abc@e,23 <-JUNK"), "abc@e,23");

    assert_eq!(strip_source(""), "");
    assert_eq!(strip_source("<"), "");
    assert_eq!(strip_source(" <"), "");
    assert_eq!(strip_source("abc@e,23<-JUNK<-MOREJUNK"), "abc@e,23");
    assert_eq!(strip_source("abc@e,23 <-JUNK<-MOREJUNK"), "abc@e,23");
}

// strip_source on a string with only a source specifier.
#[test]
fn test_removing_source_only_source() {
    assert_eq!(strip_source("<-LOGGER"), "");
}

// -----------------------------------------------------------------------
// device_name

#[test]
fn test_getting_device_name() {
    assert_eq!(device_name("abc"), "abc");
    assert_eq!(device_name("abc[]"), "abc[]");
    assert_eq!(device_name("abc@e,2"), "abc");
    assert_eq!(device_name("abc<-LOGGER"), "abc");
    assert_eq!(device_name("abc.READING"), "abc.READING");
}

// Empty string returns empty string.
#[test]
fn test_getting_device_name_empty() {
    assert_eq!(device_name(""), "");
}

// Trailing whitespace before the delimiter is trimmed.
#[test]
fn test_getting_device_name_trailing_whitespace_trimmed() {
    assert_eq!(device_name("M:OUTTMP @e,2"), "M:OUTTMP");
    assert_eq!(device_name("M:OUTTMP <-LOGGER"), "M:OUTTMP");
}

// A subscript followed by an event — the subscript is part of the name.
#[test]
fn test_getting_device_name_subscript_with_event() {
    assert_eq!(device_name("abc[3]@e,2"), "abc[3]");
}

// A subscript followed by a source — the subscript is part of the name.
#[test]
fn test_getting_device_name_subscript_with_source() {
    assert_eq!(device_name("abc[3]<-LOGGER"), "abc[3]");
}

// Both `@` and `<` present — the first delimiter wins.
#[test]
fn test_getting_device_name_event_before_source() {
    assert_eq!(device_name("abc@e,2<-LOGGER"), "abc");
}

// -----------------------------------------------------------------------
// add_event

#[test]
fn test_add_event_specification() {
    assert_eq!(add_event(None, None)("M:OUTTMP"), "M:OUTTMP@p,1000000u");
    assert_eq!(add_event(Some(1234), None)("M:OUTTMP"), "M:OUTTMP@p,1234u");

    assert_eq!(add_event(None, Some(0x02))("M:OUTTMP"), "M:OUTTMP@e,2,e");
    assert_eq!(
        add_event(Some(12345), Some(0x8f))("M:OUTTMP"),
        "M:OUTTMP@e,8F,e,12"
    );
    assert_eq!(
        add_event(Some(12499), Some(0x8f))("M:OUTTMP"),
        "M:OUTTMP@e,8F,e,12"
    );
    assert_eq!(
        add_event(Some(12500), Some(0x8f))("M:OUTTMP"),
        "M:OUTTMP@e,8F,e,13"
    );
}

// delay=Some(0) is treated as "no delay" → falls back to 1_000_000 µs.
#[test]
fn test_add_event_zero_delay_uses_default() {
    assert_eq!(add_event(Some(0), None)("M:OUTTMP"), "M:OUTTMP@p,1000000u");
}

// Rounding boundary: 499 µs rounds down to 0 ms.
#[test]
fn test_add_event_delay_rounds_down() {
    assert_eq!(
        add_event(Some(499), Some(0x01))("M:OUTTMP"),
        "M:OUTTMP@e,1,e,0"
    );
}

// Rounding boundary: 500 µs rounds up to 1 ms.
#[test]
fn test_add_event_delay_rounds_up() {
    assert_eq!(
        add_event(Some(500), Some(0x01))("M:OUTTMP"),
        "M:OUTTMP@e,1,e,1"
    );
}

// Event code 0x0F is formatted in uppercase hex.
#[test]
fn test_add_event_hex_formatting() {
    assert_eq!(add_event(None, Some(0x0f))("M:OUTTMP"), "M:OUTTMP@e,F,e");
    assert_eq!(add_event(None, Some(0xff))("M:OUTTMP"), "M:OUTTMP@e,FF,e");
}

// The closure can be applied to different device names.
#[test]
fn test_add_event_closure_reusable() {
    let ev = add_event(Some(2000), None);
    assert_eq!(ev("M:OUTTMP"), "M:OUTTMP@p,2000u");
    assert_eq!(ev("G:AMANDA"), "G:AMANDA@p,2000u");
}

// -----------------------------------------------------------------------
// flush

#[test]
fn test_flush() {
    const POINT_DATA: &[global::DataInfo] = &[
        global::DataInfo {
            timestamp: 1.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 10.0,
            }),
        },
        global::DataInfo {
            timestamp: 2.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 11.0,
            }),
        },
        global::DataInfo {
            timestamp: 3.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 12.0,
            }),
        },
        global::DataInfo {
            timestamp: 4.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 13.0,
            }),
        },
        global::DataInfo {
            timestamp: 5.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 14.0,
            }),
        },
    ];

    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: POINT_DATA.to_owned(),
        }],
    };

    ACSysSubscriptions::flush(&mut buf, 0.0);

    assert_eq!(buf.trigger_timestamp, None);
    assert_eq!(buf.data[0].channel_data, POINT_DATA);

    ACSysSubscriptions::flush(&mut buf, 3.5);

    assert_eq!(buf.trigger_timestamp, None);
    assert_eq!(buf.data[0].channel_data, &POINT_DATA[3..]);

    ACSysSubscriptions::flush(&mut buf, 10.0);

    assert_eq!(buf.trigger_timestamp, None);
    assert!(buf.data[0].channel_data.is_empty());
}

// flush on an already-empty channel_data is a no-op.
#[test]
fn test_flush_empty_channel_data_is_noop() {
    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: Some(42.0),
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![],
        }],
    };

    ACSysSubscriptions::flush(&mut buf, 5.0);

    // trigger_timestamp is always reset to None.
    assert_eq!(buf.trigger_timestamp, None);
    assert!(buf.data[0].channel_data.is_empty());
}

// flush resets trigger_timestamp to None regardless of its prior value.
#[test]
fn test_flush_resets_trigger_timestamp() {
    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: Some(99.0),
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![global::DataInfo {
                timestamp: 1.0,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 0.5,
                }),
            }],
        }],
    };

    ACSysSubscriptions::flush(&mut buf, 0.0);
    assert_eq!(buf.trigger_timestamp, None);
}

// flush with a timestamp exactly equal to a data point's timestamp
// removes that point (partition_point uses `<`, so equal timestamps
// are NOT removed — they remain).
#[test]
fn test_flush_timestamp_exactly_at_data_point() {
    let point = global::DataInfo {
        timestamp: 3.0,
        result: global::DataType::Scalar(global::Scalar { scalar_value: 1.5 }),
    };
    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![point.clone()],
        }],
    };

    // ts == point.timestamp → partition_point returns 0 (not < 3.0) →
    // nothing is drained → point survives.
    ACSysSubscriptions::flush(&mut buf, 3.0);
    assert_eq!(buf.data[0].channel_data, vec![point]);
}

// flush operates independently on each channel.
#[test]
fn test_flush_multiple_channels() {
    let make_point = |ts: f64| global::DataInfo {
        timestamp: ts,
        result: global::DataType::Scalar(global::Scalar {
            scalar_value: ts / 2.0,
        }),
    };
    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![
            types::PlotChannelData {
                channel_rate: "Unknown".into(),
                channel_units: "V".to_owned(),
                status_string: None,
                channel_status: 0,
                channel_data: vec![
                    make_point(1.0),
                    make_point(2.0),
                    make_point(3.0),
                ],
            },
            types::PlotChannelData {
                channel_rate: "Unknown".into(),
                channel_units: "A".to_owned(),
                status_string: None,
                channel_status: 0,
                channel_data: vec![make_point(10.0), make_point(20.0)],
            },
        ],
    };

    // Flush at ts=1.5: removes point at 1.0 from channel 0; channel 1
    // has no points below 1.5 so it is unchanged.
    ACSysSubscriptions::flush(&mut buf, 1.5);

    assert_eq!(
        buf.data[0].channel_data,
        vec![make_point(2.0), make_point(3.0)]
    );
    assert_eq!(
        buf.data[1].channel_data,
        vec![make_point(10.0), make_point(20.0)]
    );
}

// -----------------------------------------------------------------------
// prep_outgoing

#[test]
fn test_partitioning() {
    const POINT_DATA: &[global::DataInfo] = &[
        global::DataInfo {
            timestamp: 1.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 10.0,
            }),
        },
        global::DataInfo {
            timestamp: 2.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 11.0,
            }),
        },
        global::DataInfo {
            timestamp: 3.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 12.0,
            }),
        },
        global::DataInfo {
            timestamp: 4.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 13.0,
            }),
        },
        global::DataInfo {
            timestamp: 5.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 14.0,
            }),
        },
    ];

    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: POINT_DATA.to_owned(),
        }],
    };

    let mut rem = buf.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 0.0);

    assert!(buf.data[0].channel_data.is_empty());
    assert_eq!(buf.trigger_timestamp, Some(0.5));
    assert_eq!(rem.data[0].channel_data, POINT_DATA);

    buf = rem.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 3.5);

    assert_eq!(buf.trigger_timestamp, Some(0.5));
    assert_eq!(
        buf.data[0].channel_data,
        &[
            global::DataInfo {
                timestamp: 0.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 10.0,
                }),
            },
            global::DataInfo {
                timestamp: 1.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 11.0,
                }),
            },
            global::DataInfo {
                timestamp: 2.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 12.0,
                }),
            }
        ],
    );
    assert_eq!(rem.data[0].channel_data, &POINT_DATA[3..]);

    buf = rem.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 10.0);

    assert_eq!(buf.trigger_timestamp, Some(0.5));
    assert_eq!(
        buf.data[0].channel_data,
        &[
            global::DataInfo {
                timestamp: 3.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 13.0,
                }),
            },
            global::DataInfo {
                timestamp: 4.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 14.0,
                }),
            },
        ]
    );
    assert!(rem.data[0].channel_data.is_empty());
}

#[test]
fn test_partitioning_with_status() {
    const POINT_DATA: &[global::DataInfo] = &[
        global::DataInfo {
            timestamp: 0.75,
            result: global::DataType::StatusReply(global::StatusReply {
                status: -17 * 256 + 17,
            }),
        },
        global::DataInfo {
            timestamp: 1.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 10.0,
            }),
        },
        global::DataInfo {
            timestamp: 2.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 11.0,
            }),
        },
        global::DataInfo {
            timestamp: 3.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 12.0,
            }),
        },
        global::DataInfo {
            timestamp: 4.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 13.0,
            }),
        },
        global::DataInfo {
            timestamp: 5.0,
            result: global::DataType::Scalar(global::Scalar {
                scalar_value: 14.0,
            }),
        },
    ];

    let mut buf = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: POINT_DATA.to_owned(),
        }],
    };

    let mut rem = buf.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 0.0);

    assert!(buf.data[0].channel_data.is_empty());
    assert_eq!(buf.trigger_timestamp, Some(0.5));
    assert_eq!(rem.data[0].channel_data, POINT_DATA);

    buf = rem.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 3.5);

    assert_eq!(buf.trigger_timestamp, Some(0.5));
    assert_eq!(
        buf.data[0].channel_data,
        &[
            global::DataInfo {
                timestamp: 0.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 10.0,
                }),
            },
            global::DataInfo {
                timestamp: 1.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 11.0,
                }),
            },
            global::DataInfo {
                timestamp: 2.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 12.0,
                }),
            }
        ],
    );
    assert_eq!(rem.data[0].channel_data, &POINT_DATA[4..]);

    buf = rem.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut buf, 0.5, 10.0);

    assert_eq!(buf.trigger_timestamp, Some(0.5));
    assert_eq!(
        buf.data[0].channel_data,
        &[
            global::DataInfo {
                timestamp: 3.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 13.0,
                }),
            },
            global::DataInfo {
                timestamp: 4.5,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 14.0,
                }),
            },
        ]
    );
    assert!(rem.data[0].channel_data.is_empty());
}

// prep_outgoing with empty channel_data in both buffers — nothing moves,
// trigger_timestamp is still set.
#[test]
fn test_prep_outgoing_empty_channels() {
    let make_buf = || types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![],
        }],
    };

    let mut out = make_buf();
    let mut rem = make_buf();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut out, 5.0, 10.0);

    assert_eq!(out.trigger_timestamp, Some(5.0));
    assert!(out.data[0].channel_data.is_empty());
    assert!(rem.data[0].channel_data.is_empty());
}

// prep_outgoing sets trigger_timestamp to ev_ts on the outgoing buffer.
#[test]
fn test_prep_outgoing_sets_trigger_timestamp() {
    let make_point = |ts: f64| global::DataInfo {
        timestamp: ts,
        result: global::DataType::Scalar(global::Scalar {
            scalar_value: ts / 2.0,
        }),
    };
    let mut out = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![make_point(1.0), make_point(2.0)],
        }],
    };
    let mut rem = out.clone();

    ACSysSubscriptions::prep_outgoing(&mut rem, &mut out, 42.0, 100.0);

    assert_eq!(out.trigger_timestamp, Some(42.0));
}

// prep_outgoing with ts beyond all data — everything moves to `out`,
// `rem` is left empty.
#[test]
fn test_prep_outgoing_ts_beyond_all_data() {
    let make_point = |ts: f64| global::DataInfo {
        timestamp: ts,
        result: global::DataType::Scalar(global::Scalar {
            scalar_value: ts / 2.0,
        }),
    };
    let mut out = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![
                make_point(1.0),
                make_point(2.0),
                make_point(3.0),
            ],
        }],
    };
    let mut rem = out.clone();

    // ts = 9999.0 is beyond all data → all points go to out, rem is empty.
    ACSysSubscriptions::prep_outgoing(&mut rem, &mut out, 10.0, 9999.0);

    assert!(rem.data[0].channel_data.is_empty());
    // Timestamps are shifted by ev_ts (10.0).
    assert_eq!(
        out.data[0].channel_data,
        vec![
            global::DataInfo {
                timestamp: -9.0,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 0.5,
                }),
            },
            global::DataInfo {
                timestamp: -8.0,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 1.0,
                }),
            },
            global::DataInfo {
                timestamp: -7.0,
                result: global::DataType::Scalar(global::Scalar {
                    scalar_value: 1.5,
                }),
            },
        ]
    );
}

// prep_outgoing with ts before all data — nothing moves to out, all
// data stays in rem.
#[test]
fn test_prep_outgoing_ts_before_all_data() {
    let make_point = |ts: f64| global::DataInfo {
        timestamp: ts,
        result: global::DataType::Scalar(global::Scalar {
            scalar_value: ts / 2.0,
        }),
    };
    let mut out = types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data: vec![types::PlotChannelData {
            channel_rate: "Unknown".into(),
            channel_units: "V".to_owned(),
            status_string: None,
            channel_status: 0,
            channel_data: vec![make_point(10.0), make_point(20.0)],
        }],
    };
    let mut rem = out.clone();

    // ts = 0.0 is before all data → nothing partitioned into out.
    ACSysSubscriptions::prep_outgoing(&mut rem, &mut out, 5.0, 0.0);

    assert!(out.data[0].channel_data.is_empty());
    assert_eq!(
        rem.data[0].channel_data,
        vec![make_point(10.0), make_point(20.0)]
    );
}

// -----------------------------------------------------------------------
// build_event_descriptions / xlat_clock_event
//
// These two helpers implement the core of `clock_event()`'s behavior:
// each requested event code is given a placeholder description, and
// that description is removed from the map (and attached to the
// outgoing `AClkEvent`) the first time a matching `EventInfo` is
// translated.

fn make_event_info(event: i32, seconds: i64, nanos: i32) -> EventInfo {
    EventInfo {
        stamp: Some(Timestamp { seconds, nanos }),
        event,
    }
}

#[test]
fn test_build_event_descriptions_one_entry_per_event() {
    let desc = ACSysSubscriptions::build_event_descriptions(&[1, 2, 3]);

    assert_eq!(desc.len(), 3);
    assert!(desc.contains_key(&1));
    assert!(desc.contains_key(&2));
    assert!(desc.contains_key(&3));
}

// Duplicate event codes in the request only produce one map entry.
#[test]
fn test_build_event_descriptions_deduplicates() {
    let desc = ACSysSubscriptions::build_event_descriptions(&[5, 5, 5]);

    assert_eq!(desc.len(), 1);
    assert!(desc.contains_key(&5));
}

// An empty event list produces an empty map.
#[test]
fn test_build_event_descriptions_empty() {
    let desc = ACSysSubscriptions::build_event_descriptions(&[]);

    assert!(desc.is_empty());
}

// The first `EventInfo` for a given event code gets the description
// and the map entry is consumed (removed).
#[test]
fn test_xlat_clock_event_removes_description() {
    let mut desc = ACSysSubscriptions::build_event_descriptions(&[0x0f]);

    assert!(desc.contains_key(&0x0f));

    let out = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x0f, 100, 0),
    );

    assert_eq!(out.event, 0x0f);
    assert_eq!(out.description.as_deref(), Some("description not defined"));
    assert!(!desc.contains_key(&0x0f));
}

// A second occurrence of the same event code finds nothing left in
// the map, so its description is `None`.
#[test]
fn test_xlat_clock_event_second_occurrence_has_no_description() {
    let mut desc = ACSysSubscriptions::build_event_descriptions(&[0x0f]);

    let first = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x0f, 100, 0),
    );
    let second = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x0f, 200, 0),
    );

    assert!(first.description.is_some());
    assert!(second.description.is_none());
}

// An event code that was never requested (not present in the map)
// always comes back with no description.
#[test]
fn test_xlat_clock_event_unknown_event_has_no_description() {
    let mut desc = ACSysSubscriptions::build_event_descriptions(&[0x0f]);

    let out = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x1f, 100, 0),
    );

    assert_eq!(out.event, 0x1f);
    assert!(out.description.is_none());
    // The unrelated, requested event's description is untouched.
    assert!(desc.contains_key(&0x0f));
}

// Each event code in a multi-event subscription keeps its own
// description independent of the others.
#[test]
fn test_xlat_clock_event_independent_per_event_code() {
    let mut desc = ACSysSubscriptions::build_event_descriptions(&[0x01, 0x02]);

    let ev1 = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x01, 100, 0),
    );

    // 0x02 hasn't been seen yet, so it still has its description.
    assert!(desc.contains_key(&0x02));
    assert!(ev1.description.is_some());

    let ev2 = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x02, 200, 0),
    );

    assert!(ev2.description.is_some());
    assert!(desc.is_empty());
}

// The timestamp and event code are translated correctly, independent
// of the description-removal behavior.
#[test]
fn test_xlat_clock_event_translates_timestamp_and_event() {
    let mut desc = ACSysSubscriptions::build_event_descriptions(&[0x07]);
    let out = ACSysSubscriptions::xlat_clock_event(
        &mut desc,
        make_event_info(0x07, 1_000, 500_000_000),
    );

    assert_eq!(out.event, 0x07);
    assert_eq!(out.data, 0);
    assert_eq!(out.timestamp, 1_000.5);
}

// A missing `stamp` field results in a timestamp of 0.0 (rather than
// a panic).
#[test]
fn test_xlat_clock_event_missing_stamp_defaults_to_zero() {
    let mut desc = ACSysSubscriptions::build_event_descriptions(&[0x07]);
    let info = EventInfo {
        stamp: None,
        event: 0x07,
    };
    let out = ACSysSubscriptions::xlat_clock_event(&mut desc, info);

    assert_eq!(out.timestamp, 0.0);
}
