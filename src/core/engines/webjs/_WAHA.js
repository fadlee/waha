exports.LoadWAHA = () => {
  window.WAHA = {};
  window.WAHA.WAWebBizLabelEditingAction = window.require(
    'WAWebBizLabelEditingAction',
  );

  window.WAHA.getChats = async (pagination) => {
    let chats = window.Store.Chat.getModelsArray().slice();
    const paginator = new window.Paginator(pagination);
    chats = paginator.apply(chats);
    const chatPromises = chats.map((chat) => window.WWebJS.getChatModel(chat));
    return await Promise.all(chatPromises);
  };
};
