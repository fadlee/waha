import { Chat } from '@adiwajshing/baileys';
import { PaginationParams } from '@waha/structures/pagination.dto';
import { KnexPaginator } from '@waha/utils/Paginator';

import { IChatRepository } from '../IChatRepository';
import { NOWEBSqlite3KVRepository } from './NOWEBSqlite3KVRepository';

class ChatPaginator extends KnexPaginator {
  indexes = ['id', 'conversationTimestamp'];
}

export class Sqlite3ChatRepository
  extends NOWEBSqlite3KVRepository<Chat>
  implements IChatRepository
{
  protected Paginator = ChatPaginator;

  async getAllWithMessages(
    pagination: PaginationParams,
    broadcast: boolean,
  ): Promise<Chat[]> {
    // Get chats with conversationTimestamp is not Null
    let query = this.select().whereNotNull('conversationTimestamp');
    if (!broadcast) {
      // filter out chat by id if it ends at @newsletter or @broadcast
      query = query
        .andWhereNot('id', 'like', '%@broadcast')
        .andWhereNot('id', 'like', '%@newsletter');
    }
    query = this.pagination(query, pagination);
    return await this.all(query);
  }
}
