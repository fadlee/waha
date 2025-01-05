import { GroupMetadata } from '@adiwajshing/baileys/lib/Types/GroupMetadata';
import { IGroupRepository } from '@waha/core/engines/noweb/store/IGroupRepository';
import { KnexPaginator } from '@waha/utils/Paginator';

import { NOWEBSqlite3KVRepository } from './NOWEBSqlite3KVRepository';

class Paginator extends KnexPaginator {
  indexes = ['id'];
}

export class Sqlite3GroupRepository
  extends NOWEBSqlite3KVRepository<GroupMetadata>
  implements IGroupRepository
{
  protected Paginator = Paginator;
}
