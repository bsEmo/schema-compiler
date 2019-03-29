const inquirer = require('inquirer');
const sqlanywhere = require('sqlanywhere');
const fs = require('fs');
const dotenv = require('dotenv').config();

(async () => {
  console.log(`************************************`);
  console.log(`\tBEGIN`);
  console.log(`************************************\n`);

  if (dotenv.error) {
    console.error(dotenv.error);
    return;
  }

  console.info(dotenv);

  const answer = await inquirer
    .prompt([{
      type: 'confirm',
      message: `Proper configuration??`,
      name: "config",
      default: false
    }]);

  if (!answer.config) {
    return;
  }

  console.info("\n");
  console.info(fs.readdirSync(__dirname + "//script_to_run//"));

  const answer2 = await inquirer
    .prompt([{
      type: 'confirm',
      message: `File in there??`,
      name: "files",
      default: false
    }]);

  if (!answer2.files) {
    return;
  }

  const db = await getDbInstance();

  const fileContents = fs.readFileSync(__dirname + "//script_to_run//" + dotenv.FILE_NAME, 'utf8');

  if (dotenv.ACROSS_SCHEMAS) {
    const schemaNames = await getSchemaNames(db, dotenv.PROC_NAME);

    // MANUAL SCHEMA NAMES
    // const schemaNames = [
    //   "CIGActgS7",
    //   "CIGActgS8"
    // ];

    console.log(`Found SCHEMAS:\n${schemaNames.map(s => `\t${s}\n`)}\n`);

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
  } else {
    console.log(`Starting execution`);
    executeQuery(fileContents);
    console.log(`FINISHED execution`);
  }

  db.disconnect();
  console.log(`************************************`);
  console.log(`DONE`);
  console.log(`************************************`);
})();

function getDbInstance() {
  const connection = sqlanywhere.createConnection();

  const connectionParams = {
    Host: dotenv.DB_HOST,
    DatabaseName: dotenv.DB_NAME,
    Userid: dotenv.UID,
    Password: dotenv.PW
  };

  connection.connect(connectionParams);

  return connection;
}

async function getSchemaNames(db, procName) {
  const results = await promiseQuery(db, `exec sp_stored_procedures '%${procName}'`);

  return results.map(r => r.procedure_owner).sort();
}

async function promiseQuery(db, query) {
  return new Promise((resolve, reject) => {
    db.exec(query, (error, result) => {
      error ? reject(error) : resolve(result);
    });
  });
}