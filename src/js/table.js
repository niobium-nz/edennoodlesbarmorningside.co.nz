(function (global) {
  const DEFAULT_HEADERS = {
    Accept: 'application/json;odata=nometadata'
  };

  const DEFAULT_CONFIG = {
    retryCount: 3,
    retryDelay: 1000,
    fetchOptions: {
      method: 'GET',
      headers: DEFAULT_HEADERS,
      credentials: 'omit'
    }
  };

  function mergeOptions(overrides) {
    const options = overrides || {};
    const mergedHeaders = {
      ...DEFAULT_HEADERS,
      ...(options.fetchOptions && options.fetchOptions.headers)
    };

    return {
      retryCount: Number.isFinite(options.retryCount) ? options.retryCount : DEFAULT_CONFIG.retryCount,
      retryDelay: Number.isFinite(options.retryDelay) ? options.retryDelay : DEFAULT_CONFIG.retryDelay,
      fetchOptions: {
        ...DEFAULT_CONFIG.fetchOptions,
        ...(options.fetchOptions || {}),
        headers: mergedHeaders
      }
    };
  }

  function delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function fetchWithRetry(url, config) {
    let attempt = 0;
    let error;

    while (attempt <= config.retryCount) {
      try {
        const response = await fetch(url, config.fetchOptions);
        if (!response.ok) {
          throw new Error(`Request failed with status ${response.status}`);
        }

        const body = await response.json();
        return { body, response };
      } catch (err) {
        error = err;
        attempt += 1;
        if (attempt > config.retryCount) {
          break;
        }
        await delay(config.retryDelay * attempt);
      }
    }

    throw error;
  }

  async function fetchAllPages(url, config) {
    const records = [];
    let requestUrl = url;
    let nextPartitionKey;
    let nextRowKey;
    let continuePaging = true;

    while (continuePaging) {
      const currentUrl = new URL(requestUrl, global.location ? global.location.href : undefined);
      if (nextPartitionKey) {
        currentUrl.searchParams.set('NextPartitionKey', nextPartitionKey);
      } else {
        currentUrl.searchParams.delete('NextPartitionKey');
      }

      if (nextRowKey) {
        currentUrl.searchParams.set('NextRowKey', nextRowKey);
      } else {
        currentUrl.searchParams.delete('NextRowKey');
      }

      const { body, response } = await fetchWithRetry(currentUrl.toString(), config);
      if (Array.isArray(body.value)) {
        records.push(...body.value);
      }

      const headerNextPartitionKey = response.headers.get('x-ms-continuation-NextPartitionKey');
      const headerNextRowKey = response.headers.get('x-ms-continuation-NextRowKey');
      const bodyNextLink = body['odata.nextLink'] || body['@odata.nextLink'];

      if (bodyNextLink) {
        requestUrl = bodyNextLink;
        nextPartitionKey = null;
        nextRowKey = null;
        continuePaging = true;
      } else if (headerNextPartitionKey || headerNextRowKey) {
        requestUrl = url;
        nextPartitionKey = headerNextPartitionKey;
        nextRowKey = headerNextRowKey;
        continuePaging = true;
      } else {
        continuePaging = false;
      }
    }

    return records;
  }

  async function load(url, options = {}) {
    const config = mergeOptions(options);
    const onSuccess = typeof options.onSuccess === 'function' ? options.onSuccess : null;
    const onError = typeof options.onError === 'function' ? options.onError : null;
    const onFinally = typeof options.onFinally === 'function' ? options.onFinally : null;

    try {
      const data = await fetchAllPages(url, config);
      if (onSuccess) {
        onSuccess(data);
      }
      return data;
    } catch (error) {
      if (onError) {
        onError(error);
      } else {
          console.error('TableLoader error:', error);
      }
      throw error;
    } finally {
      if (onFinally) {
        onFinally();
      }
    }
  }

  global.TableLoader = {
    load
  };
})(window);
