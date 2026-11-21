import {api} from './client.js';
export const swapTrialsApi={list:()=>api('/swap_trials'),detail:id=>api('/swap_trials/'+id),create:data=>api('/swap_trials','POST',data)};
