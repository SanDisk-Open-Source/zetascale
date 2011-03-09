/*
 * File:   sdf/agent/00_README.txt
 * Author: Darpan Dinker
 *
 * Created on February 28, 2008, 3:48 PM
 *
 * (c) Copyright 2008, Schooner Information Technology, Inc.
 * http://www.schoonerinfotech.com/
 *
 * $Id: 00_README.txt 400 2008-02-28 23:52:53Z darpan $
 */

The agent directory should be used in SDF-Agent (aka SDF Daemon).
The main interface to incoming interprocess communication for request-response is the scoreboard.
@see sdf/shared/scoreboard.h

On receiving a request from the application (SDF client), a "thread" should call
void processMessageFromApp(struct SDFSBEntry *sbentry) as defined in agent/protocol_handler.h


