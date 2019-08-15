const inquirer = require('inquirer');
const sqlanywhere = require('sqlanywhere');
const fs = require('fs');

(async () => {
  console.log(`************************************`);
  console.log(`\tBEGIN`);
  console.log(`************************************\n`);

  const files = fs.readdirSync(__dirname + "//script_to_run//");

  const fileAnswer = await inquirer
    .prompt([{
      type: 'list',
      message: `Which file?`,
      name: "fileName",
      choices: files
    }]);

  if (!fileAnswer.fileName) {
    return;
  }

  const manyAnswers = await inquirer
    .prompt([
      {
        type: "input",
        name: "dbHost",
        message: "Please enter DB Host (format: HOST:PORT)"
      },
      {
        type: "input",
        name: "dbName",
        message: "Please enter DB Name"
      },
      {
        type: "input",
        name: "username",
        message: "Please enter DB Account Username"
      },
      {
        type: "password",
        name: "password",
        message: "Please enter DB Account Password"
      }
    ]);

  const db = await getDbInstance(manyAnswers.username, manyAnswers.password, manyAnswers.dbHost, manyAnswers.dbName);

  const schemaNames = await getSchemaNames(db, "spGlobRpt_AcctPLRollUp_Extract");

  console.log(`Found SCHEMAS:\n${schemaNames.map(s => `\t${s}\n`)}\n`);
  const schemaContinueAnswer = await inquirer
    .prompt([{
      type: 'confirm',
      message: "Continue?",
      name: "continue",
      default: false
    }]);

  if (!schemaContinueAnswer.continue) {
    return;
  }

  const fileContents = fs.readFileSync(__dirname + "//script_to_run//" + fileAnswer.fileName, 'utf8');

  for (let index = 0; index < schemaNames.length; index++) {
    let mutableFileContents = String(fileContents);
    const schemaName = schemaNames[index];

    console.log(`Starting execution on: ${schemaName} @${new Date()}`);
    mutableFileContents = mutableFileContents.replace(/cigactgs0/gi, schemaName);

    let statements = mutableFileContents.split(/\bgo\b/ig);

    for (let sIndex = 0; sIndex < statements.length; sIndex++) {
      const statement = statements[sIndex];

      if (statement.trim().length > 0) {
        await db.exec(statement);
      }
    }

    console.log(`FINISHED execution on: ${schemaName} @${new Date()}\n`);
  }

  db.disconnect();
  console.log(`************************************`);
  console.log(`DONE`);
  console.log(`************************************`);
})();

function getDbInstance(username, password, host, dbName) {
  const connection = sqlanywhere.createConnection();

  const connectionParams = {
    Host: host,
    DatabaseName: dbName,
    Userid: username,
    Password: password
  };

  connection.connect(connectionParams);

  return connection;
}

async function getSchemaNames(db, procName) {
  const results = await promiseQuery(db, `exec sp_stored_procedures '%${procName}'`);

  return results.map(r => r.procedure_owner).sort((a, b) => parseInt(a.substring(8)) - parseInt(b.substring(8)));
}

async function promiseQuery(db, query) {
  return new Promise((resolve, reject) => {
    db.exec(query, (error, result) => {
      error ? reject(error) : resolve(result);
    });
  });
}
