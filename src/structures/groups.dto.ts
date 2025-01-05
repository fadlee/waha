import { ApiProperty } from '@nestjs/swagger';
import { BooleanString } from '@waha/nestjs/validation/BooleanString';
import { Transform } from 'class-transformer';
import {
  IsArray,
  IsBoolean,
  IsEnum,
  IsOptional,
  IsString,
} from 'class-validator';

import { PaginationParams } from './pagination.dto';

/**
 * Structures
 */
export class Participant {
  @IsString()
  @ApiProperty({
    example: '123456789@c.us',
  })
  id: string;
}

export class SettingsSecurityChangeInfo {
  adminsOnly: boolean = true;
}

/**
 * Queries
 */

/**
 * Requests
 */

export class ParticipantsRequest {
  @IsArray()
  participants: Array<Participant>;
}

export class DescriptionRequest {
  @IsString()
  description: string;
}

export class SubjectRequest {
  @IsString()
  subject: string;
}

export class CreateGroupRequest {
  @IsString()
  name: string;

  @IsArray()
  participants: Array<Participant>;
}

export class JoinGroupRequest {
  @ApiProperty({
    description: 'Group code (123) or url (https://chat.whatsapp.com/123)',
    example: 'https://chat.whatsapp.com/1234567890abcdef',
  })
  @IsString()
  code: string;
}

export class JoinGroupResponse {
  @ApiProperty({
    description: 'Group ID',
    example: '123@g.us',
  })
  id: string;
}

export enum GroupSortField {
  ID = 'id',
  SUBJECT = 'subject',
}

export class GroupsPaginationParams extends PaginationParams {
  @ApiProperty({
    description: 'Sort by field',
    enum: GroupSortField,
  })
  @IsOptional()
  @IsEnum(GroupSortField)
  sortBy?: string;
}
