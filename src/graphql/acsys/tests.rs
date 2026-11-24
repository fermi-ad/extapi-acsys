//! Tests for the acsys module

use super::*;

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
// apply_x_axis

fn scalar_point(ts: f64, value: f64) -> global::DataInfo {
    global::DataInfo {
        timestamp: ts,
        result: global::DataType::Scalar(global::Scalar {
            scalar_value: value,
        }),
    }
}

fn make_channel(
    units: &str, status: i16, data: Vec<global::DataInfo>,
) -> types::PlotChannelData {
    types::PlotChannelData {
        channel_rate: "Unknown".into(),
        channel_units: units.to_owned(),
        status_string: None,
        channel_status: status,
        channel_data: data,
    }
}

fn make_reply(data: Vec<types::PlotChannelData>) -> types::PlotReplyData {
    types::PlotReplyData {
        plot_id: "test".to_owned(),
        timestamp: 0.0,
        trigger_timestamp: None,
        data,
    }
}

// apply_x_axis removes the x-axis channel and overwrites the
// timestamps of the remaining channels, positionally, with the
// x-axis channel's scalar values.
#[test]
fn test_apply_x_axis_success() {
    let mut reply = make_reply(vec![
        make_channel(
            "V",
            0,
            vec![scalar_point(100.0, 1.0), scalar_point(101.0, 2.0)],
        ),
        make_channel(
            "A",
            0,
            vec![scalar_point(10.0, 7.0), scalar_point(20.0, 8.0)],
        ),
    ]);

    // Insert the x-axis channel at index 1 (between the two device
    // channels) to confirm the function works regardless of position.

    reply.data.insert(
        1,
        make_channel(
            "x",
            0,
            vec![scalar_point(0.0, 42.0), scalar_point(0.0, 43.0)],
        ),
    );

    assert!(ACSysSubscriptions::apply_x_axis(&mut reply, 1).is_ok());

    // The x-axis channel is gone; only the two original channels
    // remain, in their original order.

    assert_eq!(reply.data.len(), 2);
    assert_eq!(
        reply.data[0].channel_data,
        vec![scalar_point(42.0, 1.0), scalar_point(43.0, 2.0)]
    );
    assert_eq!(
        reply.data[1].channel_data,
        vec![scalar_point(42.0, 7.0), scalar_point(43.0, 8.0)]
    );
}

// A non-Scalar reading from the x-axis device is a fatal error.
#[test]
fn test_apply_x_axis_rejects_non_scalar() {
    let mut reply = make_reply(vec![
        make_channel(
            "x",
            0,
            vec![global::DataInfo {
                timestamp: 0.0,
                result: global::DataType::ScalarArray(global::ScalarArray {
                    scalar_array_value: vec![1.0, 2.0],
                }),
            }],
        ),
        make_channel("V", 0, vec![scalar_point(100.0, 1.0)]),
    ]);

    let err = ACSysSubscriptions::apply_x_axis(&mut reply, 0).unwrap_err();
    assert!(err.contains("Scalar"), "{err}");
}

// A bad status on the x-axis device's channel is a fatal error.
#[test]
fn test_apply_x_axis_rejects_bad_status() {
    let mut reply = make_reply(vec![
        make_channel("x", -5, vec![]),
        make_channel("V", 0, vec![scalar_point(100.0, 1.0)]),
    ]);

    let err = ACSysSubscriptions::apply_x_axis(&mut reply, 0).unwrap_err();
    assert!(err.contains("-5"), "{err}");
}

// If a device channel has fewer (or more) readings than the x-axis
// channel, it is truncated to the shorter length instead of panicking.
#[test]
fn test_apply_x_axis_truncates_mismatched_lengths() {
    let mut reply = make_reply(vec![
        make_channel(
            "x",
            0,
            vec![
                scalar_point(0.0, 10.0),
                scalar_point(0.0, 20.0),
                scalar_point(0.0, 30.0),
            ],
        ),
        // Fewer readings than the x-axis channel.
        make_channel("V", 0, vec![scalar_point(100.0, 1.0)]),
        // More readings than the x-axis channel.
        make_channel(
            "A",
            0,
            vec![
                scalar_point(1.0, 1.0),
                scalar_point(2.0, 2.0),
                scalar_point(3.0, 3.0),
                scalar_point(4.0, 4.0),
            ],
        ),
    ]);

    assert!(ACSysSubscriptions::apply_x_axis(&mut reply, 0).is_ok());

    assert_eq!(reply.data.len(), 2);
    assert_eq!(reply.data[0].channel_data, vec![scalar_point(10.0, 1.0)]);
    assert_eq!(
        reply.data[1].channel_data,
        vec![
            scalar_point(10.0, 1.0),
            scalar_point(20.0, 2.0),
            scalar_point(30.0, 3.0),
        ]
    );
}
