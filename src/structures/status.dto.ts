import { ApiExtraModels, ApiProperty, getSchemaPath } from '@nestjs/swagger';

import {
  BinaryFile,
  RemoteFile,
  VideoBinaryFile,
  VideoRemoteFile,
  VoiceBinaryFile,
  VoiceRemoteFile,
} from './files.dto';

export const BROADCAST_ID = 'status@broadcast';

const ContactsProperty = ApiProperty({
  description: 'Contact list to send the status to.',
  example: null,
  required: false,
});

export interface StatusRequest {
  contacts?: string[];
}

export class TextStatus {
  text: string = 'Have a look! https://github.com/';
  backgroundColor: string = '#38b42f';
  font: number = 1;

  @ContactsProperty
  contacts?: string[];
}

@ApiExtraModels(RemoteFile, BinaryFile)
export class ImageStatus {
  @ApiProperty({
    oneOf: [
      { $ref: getSchemaPath(RemoteFile) },
      { $ref: getSchemaPath(BinaryFile) },
    ],
  })
  file: RemoteFile | BinaryFile;

  caption?: string;

  @ContactsProperty
  contacts?: string[];
}

@ApiExtraModels(VoiceRemoteFile, VoiceBinaryFile)
export class VoiceStatus {
  @ApiProperty({
    oneOf: [
      { $ref: getSchemaPath(VoiceRemoteFile) },
      { $ref: getSchemaPath(VoiceBinaryFile) },
    ],
  })
  file: VoiceRemoteFile | VoiceBinaryFile;

  backgroundColor: string = '#38b42f';

  @ContactsProperty
  contacts?: string[];
}

@ApiExtraModels(VideoRemoteFile, VideoBinaryFile)
export class VideoStatus {
  @ApiProperty({
    oneOf: [
      { $ref: getSchemaPath(VideoRemoteFile) },
      { $ref: getSchemaPath(VideoBinaryFile) },
    ],
  })
  file: VideoRemoteFile | VideoBinaryFile;

  caption?: string;

  @ContactsProperty
  contacts?: string[];
}

export class DeleteStatusRequest {
  @ApiProperty({
    description: 'status message id',
    example: 'AAAAAAAAAAAAAAAAA',
  })
  id: string;

  @ContactsProperty
  contacts?: string[];
}
