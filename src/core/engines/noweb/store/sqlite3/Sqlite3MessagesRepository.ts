import { ALL_JID } from '@waha/core/engines/noweb/session.noweb.core';
import { GetChatMessagesFilter } from '@waha/structures/chats.dto';
import { PaginationParams } from '@waha/structures/pagination.dto';

import { IMessagesRepository } from '../IMessagesRepository';
import { NOWEBSqlite3KVRepository } from './NOWEBSqlite3KVRepository';

export class Sqlite3MessagesRepository
  extends NOWEBSqlite3KVRepository<any>
  implements IMessagesRepository
{
  upsert(messages: any[]): Promise<void> {
    return this.upsertMany(messages);
  }

  async getAllByJid(
    jid: string,
    filter: GetChatMessagesFilter,
    pagination: PaginationParams,
  ): Promise<any[]> {
    let query = this.select();
    if (jid !== ALL_JID) {
      query = this.select().where({ jid: jid });
    }
    if (filter['filter.timestamp.lte'] != null) {
      query = query.where(
        'messageTimestamp',
        '<=',
        filter['filter.timestamp.lte'],
      );
    }
    if (filter['filter.timestamp.gte'] != null) {
      query = query.where(
        'messageTimestamp',
        '>=',
        filter['filter.timestamp.gte'],
      );
    }
    if (filter['filter.fromMe'] != null) {
      // filter by data->"$.key.fromMe"
      query = query.whereRaw("data->'$.key.fromMe' = ?", [
        filter['filter.fromMe'] ? 'true' : 'false',
      ]);
    }
    query = this.pagination(query, pagination);
    return this.all(query);
  }

  async getByJidById(jid: string, id: string): Promise<any> {
    return this.getBy({ jid: jid, id: id });
  }

  async updateByJidAndId(
    jid: string,
    id: string,
    update: any,
  ): Promise<boolean> {
    const entity = await this.getByJidById(jid, id);
    if (!entity) {
      return false;
    }
    Object.assign(entity, update);
    await this.upsertOne(entity);
  }

  async deleteByJidByIds(jid: string, ids: string[]): Promise<void> {
    const query = `DELETE
                   FROM ${this.table}
                   WHERE jid = ?
                     AND id IN (${ids.map(() => '?').join(', ')})`;
    this.db.prepare(query).run([jid, ...ids]);
  }

  deleteAllByJid(jid: string): Promise<void> {
    return this.deleteBy({ jid: jid });
  }
}
