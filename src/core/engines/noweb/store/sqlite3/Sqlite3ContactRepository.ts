import { Contact } from '@adiwajshing/baileys';
import { KnexPaginator } from '@waha/utils/Paginator';

import { IContactRepository } from '../IContactRepository';
import { NOWEBSqlite3KVRepository } from './NOWEBSqlite3KVRepository';

class ContactPaginator extends KnexPaginator {
  indexes = ['id'];
}

export class Sqlite3ContactRepository
  extends NOWEBSqlite3KVRepository<Contact>
  implements IContactRepository
{
  protected Paginator = ContactPaginator;
}
