import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Put,
  Query,
  UsePipes,
  ValidationPipe,
} from '@nestjs/common';
import { ApiOperation, ApiSecurity, ApiTags } from '@nestjs/swagger';
import { GroupIdApiParam } from '@waha/nestjs/params/ChatIdApiParam';
import {
  SessionApiParam,
  WorkingSessionParam,
} from '@waha/nestjs/params/SessionApiParam';

import { SessionManager } from '../core/abc/manager.abc';
import { WhatsappSession } from '../core/abc/session.abc';
import {
  CreateGroupRequest,
  DescriptionRequest,
  GroupsPaginationParams,
  JoinGroupRequest,
  JoinGroupResponse,
  ParticipantsRequest,
  SettingsSecurityChangeInfo,
  SubjectRequest,
} from '../structures/groups.dto';

@ApiSecurity('api_key')
@Controller('api/:session/groups')
@ApiTags('👥 Groups')
export class GroupsController {
  constructor(private manager: SessionManager) {}

  @Post('')
  @SessionApiParam
  @ApiOperation({ summary: 'Create a new group.' })
  createGroup(
    @WorkingSessionParam session: WhatsappSession,
    @Body() request: CreateGroupRequest,
  ) {
    return session.createGroup(request);
  }

  @Get('join-info')
  @SessionApiParam
  @ApiOperation({ summary: 'Get info about the group before joining.' })
  async joinInfoGroup(
    @WorkingSessionParam session: WhatsappSession,
    @Query() query: JoinGroupRequest,
  ): Promise<any> {
    // https://chat.whatsapp.com/123 => 123
    const code = query.code.split('/').pop();
    return session.joinInfoGroup(code);
  }

  @Post('join')
  @SessionApiParam
  @ApiOperation({ summary: 'Join group via code' })
  async joinGroup(
    @WorkingSessionParam session: WhatsappSession,
    @Body() request: JoinGroupRequest,
  ): Promise<JoinGroupResponse> {
    // https://chat.whatsapp.com/123 => 123
    const code = request.code.split('/').pop();
    const id = await session.joinGroup(code);
    return { id: id };
  }

  @Get('')
  @SessionApiParam
  @ApiOperation({ summary: 'Get all groups.' })
  @UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
  getGroups(
    @WorkingSessionParam session: WhatsappSession,
    @Query() pagination: GroupsPaginationParams,
  ) {
    return session.getGroups(pagination);
  }

  @Post('refresh')
  @SessionApiParam
  @ApiOperation({ summary: 'Refresh groups from the server.' })
  @UsePipes(new ValidationPipe({ transform: true, whitelist: true }))
  async refreshGroups(@WorkingSessionParam session: WhatsappSession) {
    return { success: await session.refreshGroups() };
  }

  @Get(':id')
  @GroupIdApiParam
  @SessionApiParam
  @ApiOperation({ summary: 'Get the group.' })
  getGroup(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ) {
    return session.getGroup(id);
  }

  @Put(':id/settings/security/info-admin-only')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary: 'Updates the group "info admin only" settings.',
    description:
      'You can allow only admins to edit group info (title, description, photo).',
  })
  setInfoAdminOnly(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: SettingsSecurityChangeInfo,
  ) {
    return session.setInfoAdminsOnly(id, request.adminsOnly);
  }

  @Get(':id/settings/security/info-admin-only')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary: "Get the group's 'info admin only' settings.",
    description:
      'You can allow only admins to edit group info (title, description, photo).',
  })
  getInfoAdminOnly(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ): Promise<SettingsSecurityChangeInfo> {
    return session.getInfoAdminsOnly(id);
  }

  @Put(':id/settings/security/messages-admin-only')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary: 'Update settings - who can send messages',
    description:
      'Updates the group settings to only allow admins to send messages.',
  })
  setMessagesAdminOnly(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: SettingsSecurityChangeInfo,
  ) {
    return session.setMessagesAdminsOnly(id, request.adminsOnly);
  }

  @Get(':id/settings/security/messages-admin-only')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary: 'Get settings - who can send messages',
    description: 'The group settings to only allow admins to send messages.',
  })
  getMessagesAdminOnly(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ): Promise<SettingsSecurityChangeInfo> {
    return session.getMessagesAdminsOnly(id);
  }

  @Delete(':id')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Delete the group.' })
  deleteGroup(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ) {
    return session.deleteGroup(id);
  }

  @Post(':id/leave')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Leave the group.' })
  leaveGroup(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ) {
    return session.leaveGroup(id);
  }

  @Put(':id/description')
  @ApiOperation({
    summary: 'Updates the group description.',
    description:
      'Returns "true" if the subject was properly updated. This can return "false" if the user does not have the necessary permissions.',
  })
  @SessionApiParam
  @GroupIdApiParam
  setDescription(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: DescriptionRequest,
  ) {
    return session.setDescription(id, request.description);
  }

  @Put(':id/subject')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary: 'Updates the group subject',
    description:
      'Returns "true" if the subject was properly updated. This can return "false" if the user does not have the necessary permissions.',
  })
  setSubject(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: SubjectRequest,
  ) {
    return session.setSubject(id, request.subject);
  }

  @Get(':id/invite-code')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Gets the invite code for the group.' })
  getInviteCode(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ): Promise<string> {
    return session.getInviteCode(id);
  }

  @Post(':id/invite-code/revoke')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary:
      'Invalidates the current group invite code and generates a new one.',
  })
  revokeInviteCode(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ): Promise<string> {
    return session.revokeInviteCode(id);
  }

  @Get(':id/participants/')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Get participants' })
  getParticipants(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
  ) {
    return session.getParticipants(id);
  }

  @Post(':id/participants/add')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Add participants' })
  addParticipants(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: ParticipantsRequest,
  ) {
    return session.addParticipants(id, request);
  }

  @Post(':id/participants/remove')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({
    summary: 'Remove participants',
  })
  removeParticipants(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: ParticipantsRequest,
  ) {
    return session.removeParticipants(id, request);
  }

  @Post(':id/admin/promote')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Promote participants to admin users.' })
  promoteToAdmin(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: ParticipantsRequest,
  ) {
    return session.promoteParticipantsToAdmin(id, request);
  }

  @Post(':id/admin/demote')
  @SessionApiParam
  @GroupIdApiParam
  @ApiOperation({ summary: 'Demotes participants to regular users.' })
  demoteToAdmin(
    @WorkingSessionParam session: WhatsappSession,
    @Param('id') id: string,
    @Body() request: ParticipantsRequest,
  ) {
    return session.demoteParticipantsToUser(id, request);
  }
}
