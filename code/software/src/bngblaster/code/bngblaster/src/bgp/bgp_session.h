/*
 * BNG Blaster (BBL) - BGP Session Functions
 *
 * Christian Giese, March 2022
 *
 * Copyright (C) 2020-2026, RtBrick, Inc.
 * SPDX-License-Identifier: BSD-3-Clause
 */
#ifndef __BBL_BGP_SESSION_H__
#define __BBL_BGP_SESSION_H__

const char *
bgp_session_state_string(bgp_state_t state);

void
bgp_session_update_job(timer_s *timer);

/* Identify the first UPDATE segment immediately before a RAW socket send. */
bgp_session_s *
bgp_raw_update_tx_candidate(bbl_interface_s *interface, uint8_t *packet, uint16_t length);

/* Publish a timestamp only after the complete frame was accepted by sendto. */
void
bgp_raw_update_tx_record(bgp_session_s *session, const struct timespec *attempt);

void
bgp_session_state_change(bgp_session_s *session, bgp_state_t new_state);

void
bgp_session_connect(bgp_session_s *session, time_t delay);

void
bgp_session_close(bgp_session_s *session);

void
bgp_restart_hold_timer(bgp_session_s *session, time_t timeout);

#endif
