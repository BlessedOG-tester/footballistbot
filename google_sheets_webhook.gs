const SPREADSHEET_ID = '1LCK7YS9J1hJvLc73Y088liTvZ1KvDNr6TFG-8jePrJQ';

function doPost(e) {
  try {
    const payload = JSON.parse(e.postData.contents);
    const spreadsheet = SpreadsheetApp.openById(SPREADSHEET_ID);

    if (payload.action === 'event') {
      appendEvent(spreadsheet, payload);
    } else if (payload.action === 'report') {
      appendReport(spreadsheet, payload);
    } else {
      throw new Error('Unknown action: ' + payload.action);
    }

    return jsonResponse({ ok: true });
  } catch (error) {
    return jsonResponse({ ok: false, error: String(error) });
  }
}

function appendEvent(spreadsheet, payload) {
  const event = payload.event || {};
  const gamesSheet = spreadsheet.getSheets()[0];
  const attendanceSheet = spreadsheet.getSheets()[1];

  const present = event.present || [];
  const noShows = event.no_shows || [];
  const reserve = event.reserve || [];
  const guests = event.guests || [];
  const playersText = present.map(personLabel).join(', ');

  gamesSheet.appendRow([
    event.date || '',
    event.time || '',
    event.field || '',
    present.length,
    noShows.length,
    reserve.length,
    playersText,
    guests.join(', '),
    payload.chat_id || '',
    payload.chat_title || '',
  ]);

  appendPeopleRows(attendanceSheet, payload, event, present, 'Присутствовал');
  appendPeopleRows(attendanceSheet, payload, event, noShows, 'No-show');
  appendPeopleRows(attendanceSheet, payload, event, reserve, 'Резерв');
}

function appendPeopleRows(sheet, payload, event, people, status) {
  people.forEach(person => {
    sheet.appendRow([
      event.date || '',
      event.time || '',
      event.field || '',
      person.display_name || '',
      person.username || '',
      status,
      '',
      'bot',
      payload.chat_id || '',
      payload.chat_title || '',
    ]);
  });
}

function appendReport(spreadsheet, payload) {
  const sheet = spreadsheet.getSheets()[2];
  const rows = payload.rows || [];

  rows.forEach(row => {
    sheet.appendRow([
      payload.generated_at || '',
      payload.period || '',
      row.display_name || '',
      row.username || '',
      row.visits || 0,
      row.no_shows || 0,
      row.reserve || 0,
      payload.games_count || 0,
    ]);
  });
}

function personLabel(person) {
  if (!person) {
    return '';
  }
  return person.username ? `${person.display_name} (@${person.username})` : person.display_name;
}

function jsonResponse(payload) {
  return ContentService
    .createTextOutput(JSON.stringify(payload))
    .setMimeType(ContentService.MimeType.JSON);
}
