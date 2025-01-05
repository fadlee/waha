import { PaginationParams } from '@waha/structures/pagination.dto';
import * as lodash from 'lodash';

export abstract class Paginator {
  constructor(protected readonly pagination: PaginationParams) {}

  apply<T>(data: T): T {
    if (lodash.isEmpty(this.pagination)) {
      return data;
    }
    return this.limit(this.sort(data));
  }

  protected abstract sort(data: any);

  protected abstract limit(data: any);
}

export class PaginatorInMemory extends Paginator {
  protected sort(data: any[]) {
    if (!this.pagination?.sortBy) {
      return data;
    }
    return lodash.orderBy(
      data,
      this.pagination.sortBy,
      this.pagination.sortOrder || 'asc',
    );
  }

  protected limit(data: any[]) {
    if (!this.pagination?.limit) {
      return data;
    }
    const offset = this.pagination.offset || 0;
    const limit = this.pagination.limit || Infinity;
    return data.slice(offset, offset + limit);
  }
}

export class KnexPaginator extends Paginator {
  indexes: string[] = [];
  dataField: string = 'data';

  protected sort(query: any) {
    if (!this.pagination?.sortBy) {
      return query;
    }
    const sortBy = this.pagination.sortBy;
    const direction = this.pagination.sortOrder || 'asc';
    if (this.indexes.includes(sortBy)) {
      return query.orderBy(sortBy, direction);
    }
    // Use data->"$.{field}"
    // Make sure to sanitize sortBy to prevent SQL injection
    // sqlite3 doesn't support binding for column names and direction
    return query.orderByRaw(`${this.dataField}->'$.${sortBy}' ${direction}`);
  }

  protected limit(query: any) {
    const limit = this.pagination.limit;
    const offset = this.pagination.offset;
    if (limit != null) {
      query = query.limit(limit);
    }
    if (offset != null) {
      query = query.offset(offset);
    }
    return query;
  }
}

export class MongoPaginator extends Paginator {
  protected sort(query: any) {
    if (!this.pagination?.sortBy) {
      return query;
    }
    const mongoDirection = this.pagination.sortOrder === 'asc' ? 1 : -1;
    return query.sort({ [this.pagination.sortBy]: mongoDirection });
  }

  protected limit(query: any) {
    const limit = this.pagination.limit;
    const offset = this.pagination.offset;
    if (limit != null) {
      query = query.limit(limit);
    }
    if (offset != null) {
      query = query.skip(offset);
    }
    return query;
  }
}
