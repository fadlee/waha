exports.LoadPaginator = () => {
  class Paginator {
    constructor(pagination) {
      this.pagination = pagination;
    }

    apply(data) {
      if (window.lodash.isEmpty(this.pagination)) {
        return data;
      }
      return this.limit(this.sort(data));
    }

    sort(data) {
      if (!this.pagination?.sortBy) {
        return data;
      }
      return window.lodash.orderBy(
        data,
        [this.pagination.sortBy],
        [this.pagination.sortOrder || 'asc'],
      );
    }

    limit(data) {
      if (!this.pagination?.limit) {
        return data;
      }
      const offset = this.pagination.offset || 0;
      const limit = this.pagination.limit || Infinity;
      return data.slice(offset, offset + limit);
    }
  }
  window.Paginator = Paginator;
};
